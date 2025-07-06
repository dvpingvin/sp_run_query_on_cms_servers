USE msdb;
GO

CREATE PROCEDURE dbo.sp_run_query_on_cms_servers
    @GroupName NVARCHAR(128),
    @Query NVARCHAR(MAX),
    @Timeout INT = 30,
    @SafeMode BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Объявляем переменные для аудита
    DECLARE @AuditID INT;
    DECLARE @IsDangerous BIT = 0;
    DECLARE @ErrorMessage NVARCHAR(MAX) = NULL;
    
    -- Проверка 1: @GroupName не может быть NULL или пустым
    IF @GroupName IS NULL OR LTRIM(RTRIM(@GroupName)) = ''
    BEGIN
        SET @ErrorMessage = 'Ошибка: Параметр @GroupName обязателен. Укажите имя группы или ''ALL''.';
        RAISERROR(@ErrorMessage, 16, 1);
        
        INSERT INTO dbo.cms_query_audit (
            group_name, query_text, safe_mode, timeout_sec, is_dangerous, error_message
        ) VALUES (
            @GroupName, @Query, @SafeMode, @Timeout, @IsDangerous, @ErrorMessage
        );
        RETURN;
    END;

    -- Проверка 2: Если не "ALL", проверяем существование группы
    DECLARE @GroupExists BIT = 0;
    DECLARE @GroupHasServers BIT = 0;
    
    IF UPPER(@GroupName) <> 'ALL'
    BEGIN
        SELECT @GroupExists = 1
        FROM msdb.dbo.sysmanagement_shared_server_groups
        WHERE name = @GroupName;
        
        IF @GroupExists = 0
        BEGIN
            SET @ErrorMessage = CONCAT('Ошибка: Группа "', @GroupName, '" не найдена в CMS.');
            RAISERROR(@ErrorMessage, 16, 1);
            
            INSERT INTO dbo.cms_query_audit (
                group_name, query_text, safe_mode, timeout_sec, is_dangerous, error_message
            ) VALUES (
                @GroupName, @Query, @SafeMode, @Timeout, @IsDangerous, @ErrorMessage
            );
            RETURN;
        END;
        
        -- Проверка наличия серверов в группе
        SELECT @GroupHasServers = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
        FROM msdb.dbo.sysmanagement_shared_registered_servers s
        JOIN msdb.dbo.sysmanagement_shared_server_groups g ON s.server_group_id = g.server_group_id
        WHERE g.name = @GroupName;
        
        IF @GroupHasServers = 0
        BEGIN
            SET @ErrorMessage = CONCAT('Ошибка: Группа "', @GroupName, '" не содержит серверов.');
            RAISERROR(@ErrorMessage, 16, 1);
            
            INSERT INTO dbo.cms_query_audit (
                group_name, query_text, safe_mode, timeout_sec, is_dangerous, error_message
            ) VALUES (
                @GroupName, @Query, @SafeMode, @Timeout, @IsDangerous, @ErrorMessage
            );
            RETURN;
        END;
    END;

    -- Проверка 3: Блокировка опасных запросов (если @SafeMode = 0)
    IF @SafeMode = 0
    BEGIN
        IF UPPER(@Query) LIKE '%DROP %' 
           OR UPPER(@Query) LIKE '%TRUNCATE %'
           OR UPPER(@Query) LIKE '%DELETE FROM %'
           OR UPPER(@Query) LIKE '%ALTER DATABASE %'
           OR UPPER(@Query) LIKE '%SHUTDOWN %'
        BEGIN
            SET @IsDangerous = 1;
            SET @ErrorMessage = 'Ошибка: Запрос содержит опасные операции (DROP, TRUNCATE, DELETE, etc.). Отмена выполнения. Используйте @SafeMode = 1 для обхода.';
            RAISERROR(@ErrorMessage, 16, 1);
            
            INSERT INTO dbo.cms_query_audit (
                group_name, query_text, safe_mode, timeout_sec, is_dangerous, error_message
            ) VALUES (
                @GroupName, @Query, @SafeMode, @Timeout, @IsDangerous, @ErrorMessage
            );
            RETURN;
        END;
    END
    ELSE
    BEGIN
        PRINT 'Предупреждение: Режим SafeMode=1. Опасные запросы разрешены!';
    END;
    
    -- Логируем запуск процедуры в cms_query_audit
    INSERT INTO dbo.cms_query_audit (
        group_name, query_text, safe_mode, timeout_sec, is_dangerous, error_message
    ) VALUES (
        @GroupName, @Query, @SafeMode, @Timeout, @IsDangerous, @ErrorMessage
    );
    
    SET @AuditID = SCOPE_IDENTITY(); -- Получаем ID созданной записи аудита
    
    -- Получаем список серверов (всех или из группы)
    DECLARE @servers TABLE (server_name NVARCHAR(256));
    
    IF UPPER(@GroupName) = 'ALL'
    BEGIN
        INSERT INTO @servers
        SELECT server_name 
        FROM msdb.dbo.sysmanagement_shared_registered_servers;
    END
    ELSE
    BEGIN
        INSERT INTO @servers
        SELECT s.server_name
        FROM msdb.dbo.sysmanagement_shared_registered_servers s
        JOIN msdb.dbo.sysmanagement_shared_server_groups g ON s.server_group_id = g.server_group_id
        WHERE g.name = @GroupName;
    END;
	
	-- Результаты выполнения
    DECLARE @results TABLE (
        server_name NVARCHAR(256),
        result_text NVARCHAR(MAX),
        execution_time DATETIME DEFAULT GETDATE()
    );
    
    -- Динамический SQL с обработкой ошибок и замером времени
    DECLARE @server NVARCHAR(256);
    DECLARE @dynamic_sql NVARCHAR(MAX);
    DECLARE @start_time DATETIME2;
    DECLARE @duration INT;
    
    DECLARE server_cursor CURSOR FOR 
    SELECT server_name FROM @servers;
    
    OPEN server_cursor;
    FETCH NEXT FROM server_cursor INTO @server;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @start_time = GETDATE();
        
        BEGIN TRY
            SET @dynamic_sql = @Query;
            
            INSERT INTO @results (server_name, result_text)
            EXEC sp_executesql @dynamic_sql;
            
            -- Логируем успешное выполнение
            SET @duration = DATEDIFF(MILLISECOND, @start_time, GETDATE());
            
            INSERT INTO dbo.cms_query_log (
                audit_id, server_name, execution_time, 
                status, duration_ms
            ) VALUES (
                @AuditID, @server, @start_time,
                'Success', @duration
            );
        END TRY
        BEGIN CATCH
            -- Логируем ошибку подключения
            SET @duration = DATEDIFF(MILLISECOND, @start_time, GETDATE());
            SET @ErrorMessage = ERROR_MESSAGE();
            
            INSERT INTO dbo.cms_query_log (
                audit_id, server_name, execution_time, 
                status, error_message, duration_ms
            ) VALUES (
                @AuditID, @server, @start_time,
                'Connection Error', @ErrorMessage, @duration
            );
            
            INSERT INTO @results (server_name, result_text)
            VALUES (@server, 'Ошибка подключения: ' + @ErrorMessage);
        END CATCH
        
        FETCH NEXT FROM server_cursor INTO @server;
    END
    
    CLOSE server_cursor;
    DEALLOCATE server_cursor;

    -- Вывод результатов
    SELECT 
        server_name AS [Сервер],
        result_text AS [Результат],
        execution_time AS [Время выполнения]
    FROM @results
    ORDER BY 
        CASE WHEN result_text LIKE 'Успешно%' THEN 1 ELSE 0 END,
        server_name;
END;
GO

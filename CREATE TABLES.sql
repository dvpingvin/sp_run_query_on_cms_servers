USE msdb;
GO

-- Создаем таблицу для аудита, если её нет
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'cms_query_audit')
BEGIN
    CREATE TABLE dbo.cms_query_audit (
        audit_id INT IDENTITY(1,1) PRIMARY KEY,
        execution_time DATETIME NOT NULL DEFAULT GETDATE(),
        group_name NVARCHAR(128) NULL,
        query_text NVARCHAR(MAX) NULL,
        safe_mode BIT NULL,
        timeout_sec INT NULL,
        initiated_by NVARCHAR(128) NOT NULL DEFAULT SUSER_SNAME(),
        is_dangerous BIT NULL,
        error_message NVARCHAR(MAX) NULL
    );
END;
GO

-- Создаем таблицу для детального лога выполнения, если её нет
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'cms_query_log')
BEGIN
    CREATE TABLE dbo.cms_query_log (
        log_id INT IDENTITY(1,1) PRIMARY KEY,
        audit_id INT NOT NULL,                     -- Ссылка на запись в cms_query_audit
        server_name NVARCHAR(256) NOT NULL,        -- Имя сервера
        execution_time DATETIME NOT NULL,          -- Время выполнения
        status NVARCHAR(50) NOT NULL,              -- Успех/Ошибка
        error_message NVARCHAR(MAX) NULL,          -- Текст ошибки (если была)
        duration_ms INT NULL,                      -- Длительность выполнения (мс)
        FOREIGN KEY (audit_id) REFERENCES dbo.cms_query_audit(audit_id)
    );
END;
GO

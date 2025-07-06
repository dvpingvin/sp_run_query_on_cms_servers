-- Получить статистику выполнения по последнему запуску
SELECT 
    a.execution_time AS [Время запуска],
    a.group_name AS [Группа],
    l.server_name AS [Сервер],
    l.status AS [Статус],
    l.duration_ms AS [Длительность (мс)],
    l.error_message AS [Ошибка]
FROM 
    dbo.cms_query_audit a
JOIN 
    dbo.cms_query_log l ON a.audit_id = l.audit_id
WHERE 
    a.audit_id = 6-- (SELECT MAX(audit_id) FROM dbo.cms_query_audit)
ORDER BY 
    l.execution_time DESC;

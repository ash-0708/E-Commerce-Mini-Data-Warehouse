-- 7_procedure.sql
USE ecommerce_dw_demo;

DELIMITER $$

CREATE PROCEDURE run_etl()
BEGIN
    DECLARE v_batch_id CHAR(36);
    DECLARE v_last_loaded DATETIME;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO etl_error_log(batch_id, procedure_name, error_message)
        VALUES (v_batch_id, 'run_etl', 'SQL exception occurred, transaction rolled back');
        INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
        VALUES (v_batch_id, 'RUN', NOW(), 0, 'FAILED', 'ETL failed and rolled back');
    END;

    SET v_batch_id = UUID();
    SET v_last_loaded = COALESCE(
        (SELECT MAX(last_loaded_time) FROM etl_log WHERE status = 'SUCCESS'),
        '1900-01-01 00:00:00'
    );

    START TRANSACTION;
        CALL sp_etl_extract(v_batch_id, v_last_loaded);
        CALL sp_etl_transform(v_batch_id);
        CALL sp_etl_load(v_batch_id);
        CALL sp_etl_fact_incremental(v_batch_id);
        INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
        VALUES (v_batch_id, 'RUN', NOW(), 1, 'SUCCESS', 'ETL pipeline completed successfully');
    COMMIT;
END$$

DELIMITER ;

-- Optional scheduler (privileges may be needed)
-- SET GLOBAL event_scheduler = ON;
DROP EVENT IF EXISTS ev_run_etl_hourly;
CREATE EVENT ev_run_etl_hourly
ON SCHEDULE EVERY 1 HOUR
DO
    CALL run_etl();

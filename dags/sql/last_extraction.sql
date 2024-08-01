-- Step 1: Create the table if it does not exist
CREATE TABLE IF NOT EXISTS last_extracted (LastUpdated TIMESTAMP);
-- Step 2: Insert a value only if the table is empty
INSERT INTO last_extracted (LastUpdated)
SELECT '2010-01-01 00:00:00'
WHERE NOT EXISTS (
        SELECT 1
        FROM last_extracted
    );
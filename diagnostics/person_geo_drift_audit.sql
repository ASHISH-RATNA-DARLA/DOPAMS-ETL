-- Detect likely regressions where raw state codes have replaced standardized values.
-- Run this after `etl_persons` and before/after `update-state-country` as a quick drift check.

SELECT
    person_id,
    'present' AS address_type,
    TRIM(present_state_ut) AS state_value
FROM persons
WHERE NULLIF(TRIM(COALESCE(present_state_ut, '')), '') IS NOT NULL
  AND TRIM(present_state_ut) ~ '^[A-Z]{2,3}$'

UNION ALL

SELECT
    person_id,
    'permanent' AS address_type,
    TRIM(permanent_state_ut) AS state_value
FROM persons
WHERE NULLIF(TRIM(COALESCE(permanent_state_ut, '')), '') IS NOT NULL
  AND TRIM(permanent_state_ut) ~ '^[A-Z]{2,3}$'

ORDER BY address_type, person_id;

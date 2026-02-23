

UPDATE crimes
SET case_status = 'PT'
WHERE case_status = 'PT Cases';

UPDATE crimes
SET case_status = 'UI'
WHERE case_status = 'UI Cases';

UPDATE crimes
SET case_status = 'UI'
WHERE case_status = 'New';

UPDATE crimes
SET case_status = 'Chargesheeted'
WHERE case_status = 'Chargesheet Created';

UPDATE crimes
SET case_status = 'Compounded'
WHERE case_status = 'compounded';

UPDATE crimes
SET case_status = 'PT'
WHERE case_status = 'Pending Trial';

UPDATE crimes
SET case_status = 'UI'
WHERE case_status = 'Under Investigation';

UPDATE crimes
SET case_status = 'UI'
WHERE case_status = 'Under Trial';



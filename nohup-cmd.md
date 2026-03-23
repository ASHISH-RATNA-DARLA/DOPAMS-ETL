Here are the nohup commands for all 29 ETL processes, in the order defined in 

input.txt
. Each command activates the shared venv, runs the script, and logs output to a file:

Order 1 – hierarchy
bash
cd /data-drive/etl-process-dev/etl-hierarchy && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_hierarchy.py > etl_hierarchy.log 2>&1 &
Order 2 – crimes
bash
cd /data-drive/etl-process-dev/etl-crimes && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_crimes.py > etl_crimes.log 2>&1 &
Order 3 – class_classification
bash
cd /data-drive/etl-process-dev/section-wise-case-clarification && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 process_sections.py > process_sections.log 2>&1 &
Order 4 – case_status
bash
cd /data-drive/etl-process-dev/etl_case_status && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 update_crimes.py > update_crimes.log 2>&1 &
Order 5 – accused
bash
cd /data-drive/etl-process-dev/etl-accused && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_accused.py > etl_accused.log 2>&1 &



Order 6 – persons
bash
cd /data-drive/etl-process-dev/etl-persons && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_persons.py > etl_persons.log 2>&1 &

Order 7 – update-mandal
bash
cd /data-drive/etl-process-dev/update-mandal && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 mandal_imputation_from_address.py > mandal_imputation.log 2>&1 &

Order 8 – update-state-country
bash
cd /data-drive/etl-process-dev/update-state-country && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 update-state-country.py > update_state_country.log 2>&1 &

Order 9 – domicile_classification
bash
cd /data-drive/etl-process-dev/domicile_classification && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 domicile_classifier.py > domicile_classifier.log 2>&1 &
10 – fix_person_names
bash
cd /data-drive/etl-process-dev/fix_fullname && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 fix_person_names.py > fix_person_names.log 2>&1 &

Order 11 – full_name_fix
bash
cd /data-drive/etl-process-dev/fix_fullname && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 fix_all_fullnames.py > fix_all_fullnames.log 2>&1 &

Order 12 – name_fix
bash
cd /data-drive/etl-process-dev/fix_fullname && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 fix_name_field.py > fix_name_field.log 2>&1 &
3 – surname_fix
bash
cd /data-drive/etl-process-dev/fix_fullname && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 fix_surname_field.py > fix_surname_field.log 2>&1 &

Order 14 – properties
bash
cd /data-drive/etl-process-dev/etl-properties && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_properties.py > etl_properties.log 2>&1 &
5 – IR
bash
cd /data-drive/etl-process-dev/etl-ir && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 ir_etl.py > ir_etl.log 2>&1 &

Order 16 – Disposal
bash
cd /data-drive/etl-process-dev/etl-disposal && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_disposal.py > etl_disposal.log 2>&1 &

Order 17 – arrests
bash
cd /data-drive/etl-process-dev/etl_arrests && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_arrests.py > etl_arrests.log 2>&1 &
8 – mo_seizures
bash
cd /data-drive/etl-process-dev/etl_mo_seizures && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_mo_seizure.py > etl_mo_seizure.log 2>&1 &

Order 19 – chargesheets
bash
cd /data-drive/etl-process-dev/etl_chargesheets && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_chargesheets.py > etl_chargesheets.log 2>&1 &

Order 20 – updated_chargesheet
bash
cd /data-drive/etl-process-dev/etl_updated_chargesheet && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_update_chargesheet.py > etl_update_chargesheet.log 2>&1 &
1 – fsl_case_property
bash
cd /data-drive/etl-process-dev/etl_fsl_case_property && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl_fsl_case_property.py > etl_fsl_case_property.log 2>&1 &

Order 22 – refresh_views (1st)
bash
cd /data-drive/etl-process-dev/etl_refresh_views && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 views_refresh_sql.py > views_refresh_1.log 2>&1 &

Order 23 – brief_facts_accused
bash
cd /data-drive/etl-process-dev/brief_facts_accused && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 accused_type.py > accused_type.log 2>&1 &

Order 24 – brief_facts_drugs
bash
cd /data-drive/etl-process-dev/brief_facts_drugs && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 main.py > brief_facts_drugs.log 2>&1 &

Order 25 – drug_standardization
bash
cd /data-drive/etl-process-dev/drug_standardization && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 drug_standardization.py > drug_standardization.log 2>&1 &

Order 26 – refresh_views (2nd)
bash
cd /data-drive/etl-process-dev/etl_refresh_views && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 views_refresh_sql.py > views_refresh_2.log 2>&1 &

Order 27 – update_file_id
bash
cd /data-drive/etl-process-dev/etl-files/etl_pipeline_files && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 main_standalone.py > update_file_id.log 2>&1 &

Order 28 – files_download_media_server
bash
cd /data-drive/etl-process-dev/etl-files/etl_files_media_server && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 -m etl_files_media_server.main > files_download.log 2>&1 &

Order 29 – files_download_fir_copy (parallel)
bash
cd /data-drive/etl-process-dev/etl-files && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 etl-files.py --parallel > etl_files.log 2>&1 &

Order 30 – update_file_extensions
bash
cd /data-drive/etl-process-dev/etl-files/update_file_urls_with_extensions && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 update_file_urls_with_extensions.py > update_file_extensions.log 2>&1 &

Order 31 – refresh_views (3rd / final)
bash
cd /data-drive/etl-process-dev/etl_refresh_views && source /data-drive/etl-process-dev/venv/bin/activate && nohup python3 views_refresh_sql.py > views_refresh_final.log 2>&1 &


IMPORTANT

These processes have dependencies — they must run sequentially in order (e.g., accused before persons, all data ETLs before refresh_views). Do NOT run them all in parallel. Wait for each one to finish before starting the next, or use a sequential wrapper script.
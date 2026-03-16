#!/usr/bin/env python3

file_path = r'd:\DOPAMS\Toystack\dopams-etl-pipelines\brief_facts_drugs\extractor.py'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

old_open = content.count('{{{{')
old_close = content.count('}}}}')

content = content.replace('{{{{', '{{')
content = content.replace('}}}}', '}}')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixed brace escaping in extractor.py')
print(f'Replaced open braces: {old_open} times')
print(f'Replaced close braces: {old_close} times')

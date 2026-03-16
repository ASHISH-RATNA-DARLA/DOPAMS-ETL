#!/usr/bin/env python3

from brief_facts_drugs.extractor import EXTRACTION_PROMPT
from langchain_core.prompts import ChatPromptTemplate

try:
    prompt = ChatPromptTemplate.from_template(EXTRACTION_PROMPT)
    print('SUCCESS: Prompt template created')
    print('Template variables:', prompt.input_variables)
except Exception as e:
    print(f'ERROR: {type(e).__name__}: {e}')
    import traceback
    traceback.print_exc()

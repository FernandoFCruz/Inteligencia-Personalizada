#!/bin/bash
python -c "from app.data_pipeline.metadata_extractor import extract_schema; from app.data_pipeline.indexer import index_documents; docs=extract_schema(); index_documents(docs); print('Indexed', len(docs),'documents')"

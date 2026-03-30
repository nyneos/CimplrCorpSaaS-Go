-- Migration to add upload_link column to bank_statements table
-- Run this in your PostgreSQL database

ALTER TABLE cimplrcorpsaas.bank_statements 
ADD COLUMN IF NOT EXISTS upload_link text NULL;

-- Verify the column was added
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'cimplrcorpsaas' 
  AND table_name = 'bank_statements' 
  AND column_name = 'upload_link';

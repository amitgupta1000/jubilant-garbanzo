# jubilant-garbanzo
Document Processor GCP


List of common tasks related to document processing.

List of Tasks
Summarise - Long and Short
COA Report Analysis
Form parsing
LC parsing
Visiting Cards OCR
Multi-lang Translation

User selects an option
User uploads the files
Files are sent to a GCS Trigger Bucket where they trigger a Cloud Function
The files are processed by the CLoud Function and output is sent to a GCS Result Bucket
The program downloads the output files to a local folder
User can see a concatenated output file

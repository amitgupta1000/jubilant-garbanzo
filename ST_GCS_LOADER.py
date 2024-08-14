import os, shutil, csv
import time
from datetime import datetime
from google.cloud import storage
import streamlit as st
from google.oauth2 import service_account
import pandas as pd

# Load the service account key from a secure location
# Load the service account key from a secure location
credentials_info = st.secrets["GCP_credentials"]
credentials = service_account.Credentials.from_service_account_info(credentials_info)

storage_client = storage.Client(credentials=credentials)

def upload_files_to_gcs(local_folder, gcs_target_bucket):
    """Uploads files from a local folder to a specified GCS bucket."""
    bucket = storage_client.bucket(gcs_target_bucket)
    uploaded_prefixes = set()  # Set to keep track of uploaded filename prefixes

    for filename in os.listdir(local_folder):
        local_file_path = os.path.join(local_folder, filename)
        if os.path.isfile(local_file_path):
            try:
                blob = bucket.blob(filename)
                blob.upload_from_filename(local_file_path)
                print(f"Uploaded {filename} to gs://{gcs_target_bucket}/{filename}")
                # Add the prefix (filename without extension) to the set
                uploaded_prefixes.add(os.path.splitext(filename)[0])
            except Exception as e:
                st.error(f"Error uploading {filename}: {e}")
                continue

    return uploaded_prefixes  # Return the set of uploaded prefixes

def download_files_from_gcs(gcs_result_bucket, local_folder, uploaded_prefixes):
    """Downloads files from a specified GCS bucket to a local folder,
       only if the filenames match the uploaded prefixes."""
    bucket = storage_client.bucket(gcs_result_bucket)

    blobs = bucket.list_blobs()
    for blob in blobs:
        # Check if the blob name starts with any of the uploaded prefixes
        if any(blob.name.startswith(prefix) for prefix in uploaded_prefixes):
            try:
                local_file_path = os.path.join(local_folder, blob.name)
                # Create the local directory if it doesn't exist
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                blob.download_to_filename(local_file_path)
                print(f"Downloaded gs://{gcs_result_bucket}/{blob.name} to {local_file_path}")
            except Exception as e:
                st.error(f"Error downloading {blob.name}: {e}")
                continue

def display_csv_file(file_path):
    """Displays the content of a CSV file in Streamlit."""
    try:
        df = pd.read_csv(file_path)
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")

def display_text_file(file_path):
    """Displays the content of a text file in Streamlit."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            st.text_area("File Content:", content, height=300)  # Adjust height as needed
    except Exception as e:
        st.error(f"Error reading file: {e}")


def concatenate(local_result_folder):

    if local_result_folder is None:
        st.warning("No results to concatenate. Please upload and process files first.")
    elif not os.path.exists(local_result_folder):
        st.warning(f"The directory '{local_result_folder}' does not exist.")
    else:
        directory = local_result_folder
        print(f"directory: {directory}")

        ##concatenate csv files
        combined_output_csv = os.path.join(directory, 'combined_tables.csv')
        print(f"combined_output_csv: {combined_output_csv}")

        combined_output_csv = os.path.join(directory, 'combined_tables.csv')

        # Import CSV files from folder
        table_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        
        with open(combined_output_csv, "w", newline='', encoding = 'utf-8') as outfile:
            for i, file in enumerate(table_files):
                file_path = os.path.join(directory, file)  # Create full file path
                try:
                    with open(file_path, 'r', newline='', encoding='utf-8') as infile:
                        # Option to skip preview for large files
                        #if st.checkbox(f"Show preview for {file}"):
                        display_csv_file(file_path)
                        if i != 0:
                            infile.readline()  # Throw away header on all but first file
                            # Block copy rest of file from input to output without parsing
                            shutil.copyfileobj(infile, outfile)
                            print(f"{file_path} has been imported.")
                except Exception as e:
                    print("e")
        print(f"{combined_output_csv} has been created.")

        ##concatenate text files
        combined_output_txt = os.path.join(directory, 'combined_text.txt')
        # Import text files from folder
        text_files = [f for f in os.listdir(directory) if f.endswith('.txt')]

        with open(combined_output_txt, 'w', encoding='utf-8') as outfile:
            for i, file in enumerate(text_files):
                file_path = os.path.join(directory, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as infile:
                        # Option to skip preview for large files
                        #if st.checkbox(f"Show preview for {file}"):
                        display_text_file(file_path)

                        # Continue with concatenation
                        contents = infile.read()
                        # Write a page break before each file, except the first
                        if i > 0:
                            outfile.write('\f')
                            # Write the contents of the file to the output
                            outfile.write(contents)
                            print(f"{file_path} has been imported.")
                except Exception as e:
                    print("e")
        print(f"{combined_output_txt} has been created.")


def main():
    st.title("File Upload and Processing")

    # Create a menu for users to select options
    options = ["Select an option", "Long Summary", "Short Summary", "COA Analysis", "Visiting Cards OCR", "Form Process", "LC Analysis", "Translate"]
    selected_option = st.selectbox("Select an option:", options)

    # Set the GCS target and result buckets based on the selected option
    if selected_option == "Long Summary":
        trigger_bucket = "long_summ_process"
        result_bucket = "long_summ_result"
    elif selected_option == "Short Summary":
        trigger_bucket = "short_summ_process"
        result_bucket = "short_summ_result"
    elif selected_option == "COA Analysis":
        trigger_bucket = "coa_extract_process"
        result_bucket = "coa_extract_result"
    elif selected_option == "Visiting Cards OCR":
        trigger_bucket = "vc_extract_process"
        result_bucket = "vc_extract_result"
    elif selected_option == "Form Process":
        trigger_bucket = "form_extract_process"
        result_bucket = "form_extract_result"
    elif selected_option == "LC Analysis":
        trigger_bucket = "lc_extract_process"
        result_bucket = "lc_extract_result"
    elif selected_option == "Translate":
        trigger_bucket = "translate_process"
        result_bucket = "translate_process_result"
    else:
        trigger_bucket = None
        result_bucket = None

    # Step 1: User uploads files
    uploaded_files = st.file_uploader("Choose files to upload", accept_multiple_files=True)

    if st.button("Upload and Process"):
        if uploaded_files and trigger_bucket and result_bucket:
            # Create a local folder to store uploaded files
            current_time = datetime.now().strftime("%d%m_%H%M")
            start_folder = os.path.join('temp', f'{current_time}')  # Change this to your desired local path
            os.makedirs(start_folder, exist_ok=True)
            
            for uploaded_file in uploaded_files:
                # Save the uploaded file to the start folder
                with open(os.path.join(start_folder, uploaded_file.name), "wb") as f:
                    f.write(uploaded_file.getbuffer())
                st.success(f"Uploaded {uploaded_file.name} to {start_folder}")

            # Step 2: Create a result folder with the current date and time
          
            try:
                # Step 3: Upload files to GCS and get the prefixes of uploaded files
                uploaded_prefixes = upload_files_to_gcs(start_folder, trigger_bucket)

                # Step 4: Wait for 30 seconds
                st.info("Processing files... Please wait.")
                time.sleep(30)

                # Step 5: Download files from GCS to local result folder based on prefixes
                local_result_folder = os.path.join(start_folder, "result")
                download_files_from_gcs(result_bucket, local_result_folder, uploaded_prefixes)
                st.success(f"Files processed and downloaded to {local_result_folder}.")

                concatenate (local_result_folder)
                st.success(f"Files concatenated to {local_result_folder}.")

                # Provide download links for processed files
                for filename in os.listdir(local_result_folder):
                    file_path = os.path.join(local_result_folder, filename)
                    with open(file_path, 'rb') as f:
                        st.download_button(label=f"Download {filename}", data=f, file_name=filename)

            except Exception as e:
                st.error(f"An error occurred: {e}")
        else:
            st.warning("Please select an option and upload files.")


if __name__ == '__main__':
    main()




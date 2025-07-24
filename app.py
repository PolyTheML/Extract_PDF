import streamlit as st
import pandas as pd
import camelot
import pdfplumber
from io import BytesIO
import base64
import tempfile
import os

# --- Helper Functions ---

def clean_headers(headers):
    """
    Cleans a list of header names to handle None, blanks, and duplicates.
    This is a crucial 'Transform' step.
    """
    cleaned_headers = []
    counts = {}
    if not headers:
        return []
        
    for i, header in enumerate(headers):
        # Ensure header is a string and handle None/empty cases
        header_str = str(header).replace('\n', ' ') if header is not None else f"Unnamed_{i}"
        header_str = header_str.strip()
        if not header_str:
            header_str = f"Unnamed_{i}"

        # Ensure uniqueness by appending a count to duplicates
        if header_str in counts:
            counts[header_str] += 1
            cleaned_headers.append(f"{header_str}_{counts[header_str]}")
        else:
            counts[header_str] = 1
            cleaned_headers.append(header_str)
            
    return cleaned_headers

def to_excel(df):
    """
    Converts a pandas DataFrame to an Excel file in memory.
    """
    output = BytesIO()
    # Use xlsxwriter engine for better compatibility
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def get_table_download_link(df, filename, file_label, file_type='csv'):
    """
    Generates a link to download the DataFrame.
    """
    if file_type == 'csv':
        data = df.to_csv(index=False).encode()
        mime_type = 'text/csv'
    else: # excel
        data = to_excel(df)
        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        
    b64 = base64.b64encode(data).decode()
    href = f'<a href="data:{mime_type};base64,{b64}" download="{filename}">{file_label}</a>'
    return href

# --- Core ETL Functions ---

def extract_and_transform_camelot(pdf_path, page_range, flavor, settings):
    """
    EXTRACT & TRANSFORM for Camelot with fine-tuning settings.
    Returns a list of dictionaries.
    """
    extracted_data = []
    
    try:
        # Extract tables using Camelot with user-defined settings
        tables = camelot.read_pdf(pdf_path, pages=page_range, flavor=flavor, **settings)

        for i, table in enumerate(tables):
            df = table.df
            
            # Transform: Promote first row to header and clean it
            if not df.empty:
                # Check if the first row is a plausible header
                if df.iloc[0].notna().sum() > len(df.columns) / 2:
                    cleaned_column_names = clean_headers(df.iloc[0].tolist())
                    df.columns = cleaned_column_names
                    df = df[1:].reset_index(drop=True)
                else: # If first row is not a good header, create generic ones
                    df.columns = [f"Column_{j+1}" for j in range(len(df.columns))]

            extraction_info = {
                "page_number": table.page,
                "table_number": i + 1,
                "dataframe": df,
                "accuracy": table.parsing_report.get('accuracy'),
                "engine": "Camelot"
            }
            extracted_data.append(extraction_info)
            
    except Exception as e:
        st.error(f"An error occurred during Camelot extraction: {e}")
        
    return extracted_data

def extract_and_transform_pdfplumber(pdf_path, start_page, end_page, table_settings, show_debug):
    """
    EXTRACT & TRANSFORM for PDFPlumber, with fine-tuning settings.
    Returns a list of dictionaries.
    """
    extracted_data = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Validate page range
            num_pages = len(pdf.pages)
            if start_page > num_pages or end_page > num_pages or start_page > end_page:
                st.error(f"Invalid page range. The PDF has {num_pages} pages.")
                return []

            for page_num in range(start_page, end_page + 1):
                page = pdf.pages[page_num - 1] # pdfplumber is 0-indexed
                
                # --- Visual Debugger Integration ---
                debug_image = None
                if show_debug:
                    st.info(f"Generating visual debugger for Page {page_num}...")
                    try:
                        im = page.to_image(resolution=150)
                        im.debug_tablefinder(table_settings)
                        # Convert PIL image to bytes for display in Streamlit
                        buf = BytesIO()
                        im.save(buf, format="PNG")
                        debug_image = buf.getvalue()
                    except Exception as img_e:
                        st.warning(f"Could not generate debug image for page {page_num}. Error: {img_e}")


                # Extract tables using the provided settings
                tables = page.extract_tables(table_settings)

                for i, table_data in enumerate(tables):
                    if not table_data: continue
                    
                    # Convert list of lists to DataFrame
                    df = pd.DataFrame(table_data)
                    
                    # Transform: Promote first row to header and clean it
                    if not df.empty:
                        cleaned_column_names = clean_headers(df.iloc[0].tolist())
                        df.columns = cleaned_column_names
                        df = df[1:].reset_index(drop=True)

                    extraction_info = {
                        "page_number": page_num,
                        "table_number": i + 1,
                        "dataframe": df,
                        "debug_image": debug_image,
                        "engine": "PDFPlumber"
                    }
                    extracted_data.append(extraction_info)
                    # Prevent showing the same debug image for multiple tables on one page
                    debug_image = None 

    except Exception as e:
        st.error(f"An error occurred during PDFPlumber extraction: {e}")
        
    return extracted_data


# --- Streamlit App UI ---

st.set_page_config(layout="wide")

st.title("📄 Advanced PDF Table ETL Tool")
st.markdown("""
This app uses **Camelot** or **PDFPlumber** to **Extract** tables from a PDF. 
You can **Transform** them by fine-tuning the settings in the sidebar, and then **Load** (download) the results.
""")

with st.sidebar:
    st.header("⚙️ Controls")
    uploaded_file = st.file_uploader("1. Upload your PDF", type=['pdf'])
    
    st.markdown("---")

    # --- Engine Selection ---
    engine = st.radio(
        "2. Select Extraction Engine",
        ('Camelot', 'PDFPlumber'),
        help="Choose the library to use for table extraction."
    )
    
    st.markdown("---")
    
    # --- Conditional UI for Engine Settings ---
    camelot_settings = {}
    pdfplumber_settings = {}

    if engine == 'Camelot':
        st.subheader("Camelot Settings")
        flavor = st.radio(
            "Extraction Method (Flavor)",
            ('Lattice', 'Stream'),
            index=1, # Default to Stream as it's more common for messy PDFs
            help="**Lattice**: For tables with clear grid lines. **Stream**: For tables separated by whitespace."
        )
        
        if flavor == 'Stream':
            st.info("Tune these for column issues:")
            camelot_settings['column_tol'] = st.slider(
                "Column Tolerance", 
                min_value=0, max_value=50, value=15, 
                help="Increase to merge columns that are close together. Decrease to split columns."
            )
            camelot_settings['edge_tol'] = st.slider(
                "Edge Tolerance",
                min_value=0, max_value=500, value=50,
                help="Increase to detect text close to page edges."
            )
        else: # Lattice
            camelot_settings['line_scale'] = st.slider(
                "Line Scale",
                min_value=10, max_value=100, value=40,
                help="Increase if Camelot is not detecting fine or faint table lines."
            )

    else: # PDFPlumber
        st.subheader("PDFPlumber Settings")
        show_debug = st.checkbox("Show Visual Debugger", value=True, help="Overlay detected lines and cells on the page image. Great for fine-tuning!")
        with st.expander("Fine-tune PDFPlumber Settings"):
            # --- PDFPlumber Fine-tuning Controls ---
            pdfplumber_settings["vertical_strategy"] = st.selectbox("Vertical Strategy", ["lines", "lines_strict", "text", "explicit"], index=0)
            pdfplumber_settings["horizontal_strategy"] = st.selectbox("Horizontal Strategy", ["lines", "lines_strict", "text", "explicit"], index=0)
            pdfplumber_settings["snap_tolerance"] = st.number_input("Snap Tolerance", min_value=0, value=3, help="Tolerance for snapping text to lines.")
            pdfplumber_settings["join_tolerance"] = st.number_input("Join Tolerance", min_value=0, value=3, help="Tolerance for joining nearby lines.")
            pdfplumber_settings["text_x_tolerance"] = st.number_input("Text X Tolerance", min_value=0, value=3, help="Tolerance for aligning text horizontally into columns.")
            
    st.markdown("---")
    
    st.subheader("3. Page Selection")
    # Use text input for more flexible page ranges
    page_selection = st.text_input("Enter Page(s)", value="31", help="Enter a single page, a range (e.g., 31-33), or a list (e.g., 31,33).")
    
    st.markdown("---")
    
    process_button = st.button("🚀 Start ETL Process", type="primary")

# --- Main App Logic ---
if process_button:
    if uploaded_file is not None:
        st.info("ETL process started...")
        
        # Use a temporary file to store the uploaded PDF
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name

        extracted_tables = []
        with st.spinner(f'Extracting tables with {engine}...'):
            if engine == 'Camelot':
                extracted_tables = extract_and_transform_camelot(
                    tmp_file_path, page_selection, flavor.lower(), camelot_settings
                )
            else: # PDFPlumber
                # For PDFPlumber, we need to parse the page range manually
                try:
                    pages = page_selection.split(',')
                    start_page = int(pages[0].split('-')[0])
                    end_page = int(pages[0].split('-')[-1])
                    
                    extracted_tables = extract_and_transform_pdfplumber(
                        tmp_file_path, start_page, end_page, pdfplumber_settings, show_debug
                    )
                except ValueError:
                    st.error("Invalid page format for PDFPlumber. Please use a single page or a single range (e.g., 31 or 31-33).")
        
        # Clean up the temporary file
        os.remove(tmp_file_path)

        if not extracted_tables:
            st.warning("No tables were found in the specified page range with the selected method and settings.")
        else:
            st.success(f"ETL Process Complete! Found {len(extracted_tables)} tables using {engine}.")
            
            for table_info in extracted_tables:
                page_num = table_info["page_number"]
                table_num = table_info["table_number"]
                df = table_info["dataframe"]
                
                st.subheader(f"Table {table_num} from Page {page_num}")

                # --- Display Debug Image if available ---
                if table_info.get("debug_image"):
                    st.image(table_info["debug_image"], caption=f"PDFPlumber Visual Debugger for Page {page_num}")

                if table_info["engine"] == "Camelot":
                    accuracy = table_info["accuracy"]
                    st.write(f"Extraction Accuracy (Camelot): **{accuracy:.2f}%**")
                
                st.dataframe(df)
                
                # Download Links
                csv_filename = f"table_p{page_num}_t{table_num}.csv"
                excel_filename = f"table_p{page_num}_t{table_num}.xlsx"
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(get_table_download_link(df, csv_filename, "📥 Download as CSV"), unsafe_allow_html=True)
                with col2:
                    st.markdown(get_table_download_link(df, excel_filename, "📥 Download as Excel", file_type='excel'), unsafe_allow_html=True)
                
                st.markdown("---")
    else:
        st.warning("Please upload a PDF file to start the process.")


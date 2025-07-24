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
        header_str = str(header) if header is not None else f"Unnamed_{i}"
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

def extract_and_transform_camelot(pdf_path, start_page, end_page, flavor):
    """
    EXTRACT & TRANSFORM for Camelot.
    Returns a list of dictionaries.
    """
    extracted_data = []
    
    try:
        page_range = f"{start_page}-{end_page}"
        # Extract tables using Camelot
        tables = camelot.read_pdf(pdf_path, pages=page_range, flavor=flavor)

        for i, table in enumerate(tables):
            df = table.df
            
            # Transform: Promote first row to header and clean it
            if not df.empty:
                cleaned_column_names = clean_headers(df.iloc[0].tolist())
                df.columns = cleaned_column_names
                df = df[1:].reset_index(drop=True)

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
                
                # --- ✨ NEW: Visual Debugger Integration ---
                debug_image = None
                if show_debug:
                    st.info(f"Generating visual debugger for Page {page_num}...")
                    im = page.to_image(resolution=150)
                    im.debug_tablefinder(table_settings)
                    # Convert PIL image to bytes for display in Streamlit
                    buf = BytesIO()
                    im.save(buf, format="PNG")
                    debug_image = buf.getvalue()

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

st.title("📄 PDF Table ETL Pipeline")
st.markdown("""
This app uses **Camelot** or **PDFPlumber** to **Extract** tables from a PDF, **Transform** them into a clean format, and let you **Load** (download) the results.
""")

with st.sidebar:
    st.header("⚙️ Controls")
    uploaded_file = st.file_uploader("1. Upload your PDF", type=['pdf'])
    
    st.markdown("---")

    # --- ✨ NEW: Engine Selection ---
    engine = st.radio(
        "2. Select Extraction Engine",
        ('Camelot', 'PDFPlumber'),
        help="Choose the library to use for table extraction."
    )
    
    st.markdown("---")
    
    # --- Conditional UI for Engine Settings ---
    if engine == 'Camelot':
        st.subheader("Camelot Settings")
        flavor = st.radio(
            "Extraction Method",
            ('Lattice', 'Stream'),
            help="**Lattice**: For tables with clear grid lines. **Stream**: For tables separated by whitespace."
        )
    else: # PDFPlumber
        st.subheader("PDFPlumber Settings")
        show_debug = st.checkbox("Show Visual Debugger", help="Overlay detected lines and cells on the page image. Great for fine-tuning.")
        with st.expander("Fine-tune PDFPlumber Settings"):
            # --- ✨ NEW: PDFPlumber Fine-tuning Controls ---
            vertical_strategy = st.selectbox("Vertical Strategy", ["lines", "text", "explicit"], index=0)
            horizontal_strategy = st.selectbox("Horizontal Strategy", ["lines", "text", "explicit"], index=0)
            snap_tolerance = st.number_input("Snap Tolerance", min_value=0, value=3, help="Tolerance for snapping text to lines.")
            join_tolerance = st.number_input("Join Tolerance", min_value=0, value=3, help="Tolerance for joining nearby lines.")
            text_x_tolerance = st.number_input("Text X Tolerance", min_value=0, value=3, help="Tolerance for aligning text horizontally.")
            
    st.markdown("---")
    
    st.subheader("Page Selection")
    start_page_input = st.number_input("Start Page", min_value=1, value=1)
    end_page_input = st.number_input("End Page", min_value=1, value=1)
    
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
                    tmp_file_path, start_page_input, end_page_input, flavor.lower()
                )
            else: # PDFPlumber
                table_settings = {
                    "vertical_strategy": vertical_strategy,
                    "horizontal_strategy": horizontal_strategy,
                    "snap_tolerance": snap_tolerance,
                    "join_tolerance": join_tolerance,
                    "text_x_tolerance": text_x_tolerance,
                }
                extracted_tables = extract_and_transform_pdfplumber(
                    tmp_file_path, start_page_input, end_page_input, table_settings, show_debug
                )
        
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

                # --- ✨ NEW: Display Debug Image if available ---
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

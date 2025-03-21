import tabula
import pandas as pd
import requests

# URL of the PDF
pdf_url = "https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/legislacao/documentos-e-arquivos/tipi.pdf/@@download/file"

# Download the PDF
pdf_path = "tipi.pdf"
response = requests.get(pdf_url)
with open(pdf_path, "wb") as f:
    f.write(response.content)

# Extract all tables from the PDF
tables = tabula.read_pdf(pdf_path, pages="all", multiple_tables=True)

# Combine all tables into a single DataFrame
combined_df = pd.concat(tables, ignore_index=True)

# Save the combined DataFrame to a CSV file
combined_df.to_csv("brazil_tarrif.csv", index=False)

print("Tables extracted and saved to 'brazil_tarrif.csv'")
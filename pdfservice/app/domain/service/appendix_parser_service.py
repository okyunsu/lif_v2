from app.foundation.utils.pdf_table_parser import parse_esg_tables

class AppendixParserService:
    def __init__(self):
        pass

    def parse_pdf(self, pdf_path: str):
        return parse_esg_tables(pdf_path)

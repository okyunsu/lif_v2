from app.domain.service.appendix_parser_service import AppendixParserService

class AppendixParserController:
    def __init__(self):
        self.service = AppendixParserService()

    def parse(self, pdf_path: str):
        return self.service.parse_pdf(pdf_path)

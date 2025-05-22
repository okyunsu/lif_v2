import pdfplumber
from typing import List, Dict


def parse_esg_tables(pdf_path: str) -> List[Dict]:
    results = []

    with pdfplumber.open(pdf_path) as pdf:
        # 마지막 30페이지만 검사
        for page in pdf.pages[-30:]:
            tables = page.extract_tables()

            for table in tables:
                if not table or len(table[0]) < 3:
                    continue  # 너무 작거나 비정형 테이블 무시

                header = table[0]

                # ✅ None 방지 및 '202x' 연도 포함 여부 체크
                if not any(h and "202" in h for h in header):
                    continue

                for row in table[1:]:
                    if len(row) < 3:
                        continue  # 유효하지 않은 행은 스킵

                    row_dict = {
                        "indicator": row[0] or "",
                        "unit": row[1] or ""
                    }

                    for i in range(2, len(row)):
                        try:
                            year = header[i]
                            if not year:
                                continue
                            year = year.replace("년", "").strip()
                            value = row[i]
                            if value is None:
                                continue
                            value_cleaned = float(value.replace(",", ""))
                            row_dict[year] = value_cleaned
                        except:
                            row_dict[year] = row[i]  # 숫자 변환 안 되면 원문 유지

                    results.append(row_dict)

    return results

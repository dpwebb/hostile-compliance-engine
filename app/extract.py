import logging
from typing import List

from pypdf import PdfReader

logger = logging.getLogger(__name__)


def extract_text_pages(pdf_path: str) -> List[str]:
    reader = PdfReader(pdf_path)
    pages: List[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            logger.exception("Failed to extract text from page")
            text = ""
        pages.append(text)
    return pages


def text_quality_score(pages: List[str]) -> int:
    text = "".join(pages)
    return sum(ch.isalnum() for ch in text)


def ocr_pdf_pages(pdf_path: str) -> List[str]:
    try:
        from pdf2image import convert_from_path
        import pytesseract
    except Exception as exc:
        logger.warning("OCR dependencies unavailable: %s", exc)
        return []

    try:
        images = convert_from_path(pdf_path)
    except Exception as exc:
        logger.warning("OCR conversion failed: %s", exc)
        return []

    pages: List[str] = []
    for image in images:
        try:
            pages.append(pytesseract.image_to_string(image) or "")
        except Exception as exc:
            logger.warning("OCR page failed: %s", exc)
            pages.append("")
    return pages

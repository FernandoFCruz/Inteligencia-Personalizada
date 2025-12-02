import os
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

DOCS_DIR = "app/data_pipeline/Docs_Vettor/ref_docs"

def _clean_text(s: str):
    s = s.replace("\r", "").replace("\t", " ").strip()
    s = "\n".join([line.strip() for line in s.split("\n") if line.strip()])
    return s

def _split_chunks(text: str, max_chars=2000):
    chunks = []
    cur = ""

    for line in text.split("\n"):
        if len(cur) + len(line) + 1 < max_chars:
            cur += line + "\n"
        else:
            chunks.append(cur.strip())
            cur = line + "\n"

    if cur.strip():
        chunks.append(cur.strip())

    return chunks

def _ingest_txt(path):
    with open(path, encoding="utf-8") as f:
        text = _clean_text(f.read())
    return _split_chunks(text)


def _ingest_html(path):
    with open(path, encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = _clean_text(soup.get_text(separator="\n"))
    return _split_chunks(text)


def _ingest_pdf(path):
    reader = PdfReader(path)
    text = ""

    for page in reader.pages:
        try:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
        except Exception:
            continue

    text = _clean_text(text)
    return _split_chunks(text)


_INGESTORS = {
    ".txt": _ingest_txt,
    ".pdf": _ingest_pdf,
    ".html": _ingest_html,
    ".htm": _ingest_html,
}

def load_documents():
    docs = []

    if not os.path.exists(DOCS_DIR):
        print(f"[DOC] Diretório {DOCS_DIR} não existe, ignorando.")
        return []

    for fname in os.listdir(DOCS_DIR):
        fpath = os.path.join(DOCS_DIR, fname)
        if not os.path.isfile(fpath):
            continue

        ext = os.path.splitext(fname)[1].lower()
        ingestor = _INGESTORS.get(ext)

        if not ingestor:
            print(f"[DOC] Ignorado (extensão não suportada): {fname}")
            continue

        print(f"[DOC] Ingerindo arquivo: {fname}")
        try:
            chunks = ingestor(fpath)
            for i, chunk in enumerate(chunks):
                uid = f"{fname}:{i+1}"
                docs.append((uid, chunk))
        except Exception as e:
            print(f"[ERRO] Falha ao processar {fname}: {e}")

    print(f"[DOC] {len(docs)} chunks gerados")
    return docs
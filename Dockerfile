# ------------------------------------------------------------------------------
# Dataflow Flex-Template: launcher + SDK harness (Python 3.11)
# ------------------------------------------------------------------------------

    FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

    # ---------- build-time variable (pass via --build-arg) ----------
    ARG PROJECT_ID
    ARG WORKDIR=/template
    
    WORKDIR ${WORKDIR}
    
    # ---------- copy sources ----------
    COPY requirements.txt requirements.txt
    COPY setup.py         setup.py
    COPY nps_intercom.py  nps_intercom.py
    COPY metadata.json    metadata.json
    
    # ---------- install dependencies & local package ----------
    RUN pip install --no-cache-dir -r requirements.txt \
     && pip install --no-cache-dir .
    
    # ---------- Flex-template variables ----------
    # путь к main.py
    ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/nps_intercom.py"
    
    # setup.py передаётся в Job, чтобы Beam установил пакет и на воркерах
    ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
    
    # чтобы импорты видели /template
    ENV PYTHONPATH="${WORKDIR}"
# ----------------------------------------------------------------------------
# Dataflow Flex-Template: NPS Intercom Pipeline – launcher + SDK harness (Py 3.11)
# ----------------------------------------------------------------------------

FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

ARG PROJECT_ID
ARG WORKDIR=/template

WORKDIR ${WORKDIR}

COPY requirements.txt requirements.txt
COPY setup.py         setup.py
COPY nps_intercom.py  nps_intercom.py
COPY metadata.json    metadata.json

RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/nps_intercom.py"

ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

# Image with SDK harness layer (built separately)
ENV FLEX_TEMPLATE_PYTHON_SDK_CONTAINER_IMAGE="gcr.io/${PROJECT_ID}/nps-intercom-pipeline-sdk:1.0.0"

ENV PYTHONPATH="${WORKDIR}"
# ─────────────────────────────────────────────────────────────────────────────
# 1) 베이스 이미지 설정
FROM python:3.11-slim

# ─────────────────────────────────────────────────────────────────────────────
# 2) 시스템 패키지 설치
#    - gcc, make 등 C 컴파일러
#    - cmake (kiwipiepy 빌드 시 필요)
#    - libstdc++ (C++ 런타임)
#    - python3-dev (파이썬 헤더 파일)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      cmake \
      python3-dev \
      && rm -rf /var/lib/apt/lists/*

# Install Chromium browser and dependencies for Selenium
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      chromium \
      chromium-driver \
      libnss3 \
      libatk1.0-0 \
      libatk-bridge2.0-0 \
      libcups2 \
      libdrm2 \
      libxdamage1 \
      libxkbcommon0 \
      libxcomposite1 \
      libxrandr2 \
      libgbm1 \
      libgtk-3-0 \
      libpango-1.0-0 \
      libasound2 \
      libpangocairo-1.0-0 \
      fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Set environment variable for headless Chromium
ENV CHROME_BIN=/usr/bin/chromium

# ─────────────────────────────────────────────────────────────────────────────
# 3) 작업 디렉토리 설정
WORKDIR /app

# ─────────────────────────────────────────────────────────────────────────────
# 4) 먼저 numpy만 설치 (hdbscan, kiwipiepy 의존)
RUN pip install --no-cache-dir numpy==2.2.6

# ─────────────────────────────────────────────────────────────────────────────
# 5) requirements.txt 복사
COPY requirements.txt /app/
COPY kiwipiepy-0.21.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /app/

# ─────────────────────────────────────────────────────────────────────────────
# 6) 나머지 의존성 설치
RUN pip install --no-cache-dir /app/kiwipiepy-0.21.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl && \
    pip install --no-cache-dir -r requirements.txt

# ─────────────────────────────────────────────────────────────────────────────
# 7) 애플리케이션 코드 복사
COPY . /app

# ─────────────────────────────────────────────────────────────────────────────
# 8) FastAPI 포트 노출
EXPOSE 8000

# ─────────────────────────────────────────────────────────────────────────────
# 9) 컨테이너 시작 커맨드
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
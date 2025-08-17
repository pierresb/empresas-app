# 1) criar o projeto
mkdir cnpj_app && cd cnpj_app
# 2) criar as pastas e salvar os arquivos acima nos caminhos respectivos
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
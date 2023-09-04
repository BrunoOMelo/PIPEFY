import subprocess
import sys

# Pacotes necessários
required_packages = [
    "google-auth",
    "google-auth-oauthlib",
    "google-auth-httplib2",
    "google-api-python-client",
    "datetime"
]

for package in required_packages:
    try:
        __import__(package)
        print(f"{package} já está instalado. Atualizando agora...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", package])
        print(f"{package} foi atualizado com sucesso.")
    except ImportError:
        print(f"{package} não está instalado. Instalando agora...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"{package} foi instalado com sucesso.")
        except Exception as e:
            print(f"Erro ao instalar/atualizar {package}: {e}")
            sys.exit(1)

print("Todos os pacotes foram instalados/atualizados com sucesso!")
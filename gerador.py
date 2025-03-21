import csv
import random

registros = [
    (6, "Administrador"),
    (7, "Gerente"),
    (8, "Usuário Padrão"),
    (9, "Convidado"),
    (10, "Financeiro"),
    (11, "Recursos Humanos"),
    (12, "Suporte Técnico"),
    (13, "Desenvolvedor"),
    (14, "Analista de Dados"),
    (15, "Marketing"),
    (16, "Vendas"),
    (17, "Gestor de Projetos"),
    (18, "Operacional"),
    (19, "Auditor"),
    (21, "Treinamento"),
    (22, "Supervisor"),
    (23, "Consultor"),
    (25, "Estagiário"),
]

with open("dados.csv", "w", newline="", encoding="utf-8") as arquivo:
    escritor = csv.writer(arquivo, delimiter=';')
    escritor.writerow(["id", "nome"])
    for _ in range(1_000_000):
        escritor.writerow(random.choice(registros))

print("Arquivo CSV gerado com sucesso!")

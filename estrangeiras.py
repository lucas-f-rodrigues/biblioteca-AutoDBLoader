import pandas as pd

dados = [
    ('arquivos', 'aula_id', 'aula', 'id'), 
    ('modulo', 'turma_id', 'turma', 'id'), 
    ('feito', 'aluno_id', 'aluno', 'id'),
    ('feito', 'aula_id', 'aula', 'id'),
    ('aula', 'modulo_id', 'modulo', 'id'),
    ('usuario', 'grupo_id', 'grupo', 'id'),
    ('aluno', 'turma_id', 'turma', 'id'),
    ('aluno', 'usuario_id', 'usuario', 'id'),
    ('professor', 'turma_id', 'turma', 'id'),
    ('professor', 'usuario_id', 'usuario', 'id'),
    ('permissoes', 'grupo_id', 'grupo', 'id'),
    ('permissoes', 'rota_id', 'rota', 'id')
]

df = pd.DataFrame(dados, columns=["tabela", "chave_estrangeira", "tabela_estrangeira", "id_tabela_estrangeira"])

print(df)

dicionario = dict({"nome":df})

print(dicionario["nome"])
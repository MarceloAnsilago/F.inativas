import base64
import os
from html import escape
from pathlib import Path
import sqlite3
import unicodedata

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "meu_banco.db"
PLANILHA_PADRAO = BASE_DIR / "Fichas Inativas Novo.xlsm"
TABELA = "fichas_inativas"
EXTENSOES_PLANILHA = {".xlsm", ".xlsx", ".xls"}
COLUNAS_EXIBICAO = [
    "codigo",
    "nome",
    "cpf",
    "numero_ficha",
    "pasta",
    "numero_pasta",
    "cidade",
]
COLUNAS_RESULTADO = [
    "codigo",
    "nome",
    "cpf",
    "numero_ficha",
    "pasta",
    "numero_pasta",
    "cidade",
    "endereco",
]
ICONES_CARDS = {
    "Codigo": "🏷️",
    "Nome": "👤",
    "CPF": "📄",
    "Ficha": "📄",
    "Pasta": "📁",
    "Cidade": "📍",
    "Endereco": "🏠",
}


COLUNAS_IMPRESSAO = [
    ("Nome", "nome"),
    ("CPF", "cpf"),
    ("Cidade", "cidade"),
    ("Codigo", "codigo"),
    ("Pasta", "pasta"),
    ("Ficha", "numero_ficha"),
    ("Endereco", "endereco"),
]


def normalizar_texto(valor: object) -> str:
    texto = "" if pd.isna(valor) else str(valor).strip()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(char for char in texto if not unicodedata.combining(char))
    return " ".join(texto.upper().split())


def somente_digitos(valor: object) -> str:
    texto = "" if pd.isna(valor) else str(valor)
    return "".join(char for char in texto if char.isdigit())


def slug_coluna(valor: object) -> str:
    texto = normalizar_texto(valor).replace(" ", "_")
    return texto.strip("_")


def localizar_linha_cabecalho(df_bruto: pd.DataFrame) -> int:
    for indice, linha in df_bruto.iterrows():
        valores = {normalizar_texto(item) for item in linha.tolist() if not pd.isna(item)}
        if {"CPF", "NOME", "PASTA"}.issubset(valores):
            return indice
    raise ValueError("Nao foi possivel localizar o cabecalho da aba DADOS.")


def carregar_planilha_excel(arquivo) -> pd.DataFrame:
    df_bruto = pd.read_excel(arquivo, sheet_name="DADOS", header=None)
    cabecalho = localizar_linha_cabecalho(df_bruto)

    colunas = [slug_coluna(valor) or f"coluna_{indice}" for indice, valor in enumerate(df_bruto.iloc[cabecalho])]
    df = df_bruto.iloc[cabecalho + 1 :].copy()
    df.columns = colunas
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

    renomear = {
        "COD": "codigo",
        "LETRA": "letra",
        "CPF": "cpf",
        "NOME": "nome",
        "ENDERECO": "endereco",
        "CIDADE": "cidade",
        "PASTA": "pasta",
        "PASTA_NUMERO": "numero_pasta",
        "N": "numero_ficha",
    }
    df = df.rename(columns={origem: destino for origem, destino in renomear.items() if origem in df.columns})

    colunas_necessarias = ["codigo", "cpf", "nome", "pasta", "numero_ficha"]
    faltando = [coluna for coluna in colunas_necessarias if coluna not in df.columns]
    if faltando:
        raise ValueError(f"Colunas obrigatorias ausentes: {', '.join(faltando)}")

    for coluna in ["codigo", "numero_pasta", "numero_ficha"]:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors="coerce").astype("Int64")

    for coluna in ["cpf", "nome", "endereco", "cidade", "pasta", "letra"]:
        if coluna in df.columns:
            df[coluna] = df[coluna].fillna("").astype(str).str.strip()

    df["cpf_digitos"] = df["cpf"].map(somente_digitos)
    df["nome_busca"] = df["nome"].map(normalizar_texto)

    df = df[df["nome"] != ""].copy()
    df = df[df["cpf_digitos"] != ""].copy()
    df = df.drop_duplicates(subset=["cpf_digitos", "nome", "pasta", "numero_ficha"]).reset_index(drop=True)
    return df


def resolver_caminho_informado(caminho_informado: str) -> Path:
    caminho_limpo = caminho_informado.strip().strip('"').strip("'")
    if not caminho_limpo:
        raise ValueError("Informe uma pasta ou arquivo.")

    caminho = Path(caminho_limpo).expanduser()
    if caminho.exists():
        return caminho

    # Permite usar um caminho Windows em execucoes locais baseadas em Linux/WSL.
    if os.name != "nt" and len(caminho_limpo) >= 3 and caminho_limpo[1:3] == ":\\":
        unidade = caminho_limpo[0].lower()
        restante = caminho_limpo[3:].replace("\\", "/")
        caminho_wsl = Path(f"/mnt/{unidade}/{restante}")
        if caminho_wsl.exists():
            return caminho_wsl
        raise FileNotFoundError(
            "Caminho Windows detectado, mas este ambiente nao consegue acessar o disco local do usuario. "
            "No Streamlit Cloud use 'Enviar arquivo'."
        )

    raise FileNotFoundError(f"Caminho nao encontrado: {caminho}")


def listar_planilhas(caminho_informado: str) -> list[Path]:
    caminho = resolver_caminho_informado(caminho_informado)

    if caminho.is_file():
        if caminho.suffix.lower() not in EXTENSOES_PLANILHA:
            raise ValueError("Informe um arquivo Excel valido (.xlsm, .xlsx ou .xls).")
        return [caminho]

    planilhas = [
        arquivo
        for arquivo in caminho.iterdir()
        if arquivo.is_file() and arquivo.suffix.lower() in EXTENSOES_PLANILHA
    ]
    return sorted(planilhas, key=lambda arquivo: arquivo.name.lower())


def salvar_no_banco(df: pd.DataFrame) -> None:
    with sqlite3.connect(DB_PATH) as conexao:
        df.to_sql(TABELA, conexao, if_exists="replace", index=False)
        conexao.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABELA}_cpf ON {TABELA}(cpf_digitos)")
        conexao.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABELA}_nome ON {TABELA}(nome_busca)")
        conexao.commit()


def banco_disponivel() -> bool:
    if not DB_PATH.exists():
        return False
    with sqlite3.connect(DB_PATH) as conexao:
        consulta = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?"
        return conexao.execute(consulta, (TABELA,)).fetchone() is not None


def carregar_resumo_banco() -> tuple[int, str | None]:
    if not banco_disponivel():
        return 0, None
    with sqlite3.connect(DB_PATH) as conexao:
        total = conexao.execute(f"SELECT COUNT(*) FROM {TABELA}").fetchone()[0]
    atualizado = pd.to_datetime(DB_PATH.stat().st_mtime, unit="s").strftime("%d/%m/%Y %H:%M")
    return total, atualizado


def pesquisar_registros(termo: str) -> pd.DataFrame:
    termo_limpo = termo.strip()
    if not termo_limpo:
        return pd.DataFrame(columns=COLUNAS_RESULTADO)

    termo_nome_normalizado = normalizar_texto(termo_limpo)
    termo_cpf_digitos = somente_digitos(termo_limpo)
    filtros = []
    parametros: list[str] = []

    if termo_nome_normalizado:
        filtros.append("nome_busca LIKE ?")
        parametros.append(f"%{termo_nome_normalizado}%")

    if termo_cpf_digitos:
        filtros.append("cpf_digitos LIKE ?")
        parametros.append(f"%{termo_cpf_digitos}%")

    if not filtros:
        return pd.DataFrame(columns=COLUNAS_RESULTADO)

    consulta = f"""
        SELECT
            codigo,
            nome,
            cpf,
            numero_ficha,
            pasta,
            numero_pasta,
            cidade,
            endereco
        FROM {TABELA}
        WHERE {" OR ".join(filtros)}
        ORDER BY nome, numero_ficha
    """
    with sqlite3.connect(DB_PATH) as conexao:
        return pd.read_sql_query(consulta, conexao, params=parametros)


def valor_card(valor: object) -> str:
    if pd.isna(valor) or valor == "":
        return "-"
    return str(valor)


def iniciar_estado() -> None:
    if "termo_pesquisa" not in st.session_state:
        st.session_state["termo_pesquisa"] = ""
    if "executar_pesquisa" not in st.session_state:
        st.session_state["executar_pesquisa"] = False


def exportar_para_pesquisa(valor: object) -> None:
    st.session_state["termo_pesquisa"] = valor_card(valor)
    st.session_state["executar_pesquisa"] = True


def gerar_html_impressao(registro: pd.Series) -> str:
    dados = dados_impressao(registro)
    titulo = (
        f'{dados["nome"]} | CPF {dados["cpf"]} | '
        f'Ficha {dados["numero_ficha"]} | Pasta {dados["pasta"]}'
    )
    cards_html = []
    for titulo_card, coluna in COLUNAS_IMPRESSAO:
        classe = "print-card full" if coluna == "endereco" else "print-card"
        cards_html.append(
            (
                f'<div class="{classe}">'
                f'<p class="print-label">{escape(titulo_card)}</p>'
                f'<p class="print-value">{escape(dados[coluna])}</p>'
                "</div>"
            )
        )

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Ficha para impressao</title>
    <style>
        body {{
            margin: 0;
            background: #f3f4f6;
            color: #111827;
            font-family: Arial, sans-serif;
        }}
        .page {{
            max-width: 900px;
            margin: 0 auto;
            padding: 24px;
        }}
        .print-sheet {{
            border: 1px solid #d1d5db;
            border-radius: 16px;
            background: #ffffff;
            padding: 24px;
        }}
        .print-title {{
            margin: 0 0 16px 0;
            font-size: 2rem;
        }}
        .print-subtitle {{
            margin: 0 0 20px 0;
            font-size: 1rem;
        }}
        .print-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 12px;
        }}
        .print-card {{
            border: 1px solid #e5e7eb;
            border-radius: 14px;
            padding: 16px;
            break-inside: avoid;
        }}
        .print-card.full {{
            grid-column: 1 / -1;
        }}
        .print-label {{
            margin: 0 0 8px 0;
            color: #6b7280;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }}
        .print-value {{
            margin: 0;
            color: #111827;
            font-size: 1.05rem;
            white-space: pre-wrap;
            word-break: break-word;
        }}
        @media print {{
            body {{
                background: #ffffff;
            }}
            .page {{
                max-width: 100%;
                padding: 0;
            }}
            .print-sheet {{
                border: none;
                border-radius: 0;
                padding: 0;
            }}
        }}
    </style>
</head>
<body>
    <main class="page">
        <section class="print-sheet">
            <h1 class="print-title">Ficha para impressao</h1>
            <p class="print-subtitle">{escape(titulo)}</p>
            <div class="print-grid">{"".join(cards_html)}</div>
        </section>
    </main>
    <script>
        window.addEventListener("load", function () {{
            setTimeout(function () {{
                window.print();
            }}, 250);
        }});
    </script>
</body>
</html>"""


def render_botao_impressao_direta(registro: pd.Series) -> None:
    html_impressao = gerar_html_impressao(registro)
    payload = base64.b64encode(html_impressao.encode("utf-8")).decode("ascii")
    components.html(
        f"""
        <div style="margin-top: 0.75rem;">
            <button
                id="print-btn"
                type="button"
                style="display:inline-flex;align-items:center;justify-content:center;
                width:100%;padding:0.65rem 1rem;border-radius:0.75rem;border:1px solid #d1d5db;
                background:#ffffff;color:#111827;text-decoration:none;font-weight:600;cursor:pointer;">
                Imprimir direto
            </button>
            <div
                id="print-error"
                style="display:none;margin-top:0.5rem;color:#b91c1c;font-size:0.9rem;">
                O navegador bloqueou a nova janela. Permita pop-ups e tente novamente.
            </div>
        </div>
        <script>
            const htmlImpressaoBase64 = "{payload}";
            const botao = document.getElementById("print-btn");
            const erro = document.getElementById("print-error");

            botao.addEventListener("click", function () {{
                const popup = window.open("about:blank", "_blank");
                if (!popup) {{
                    erro.style.display = "block";
                    return;
                }}

                const htmlImpressao = decodeURIComponent(escape(window.atob(htmlImpressaoBase64)));
                popup.document.open();
                popup.document.write(htmlImpressao);
                popup.document.close();
                popup.focus();
            }});
        </script>
        """,
        height=95,
    )


def abrir_pagina_impressao(registro: pd.Series) -> None:
    st.query_params.clear()
    st.query_params["modo"] = "impressao"
    st.query_params["auto_print"] = "1"
    for coluna, valor in dados_impressao(registro).items():
        st.query_params[coluna] = valor
    st.rerun()


def voltar_para_pesquisa() -> None:
    st.query_params.clear()
    st.rerun()


def pagina_impressao_ativa() -> bool:
    return st.query_params.get("modo") == "impressao"


def dados_impressao(registro: pd.Series) -> dict[str, str]:
    return {coluna: valor_card(registro.get(coluna, "")) for _, coluna in COLUNAS_IMPRESSAO}


def render_pagina_impressao() -> None:
    st.markdown(
        """
        <style>
            [data-testid="stHeader"],
            [data-testid="stToolbar"],
            [data-testid="stDecoration"],
            [data-testid="collapsedControl"] {
                display: none !important;
            }
            .block-container {
                max-width: 900px;
                padding-top: 1.5rem;
                padding-bottom: 2rem;
            }
            .print-actions {
                display: flex;
                gap: 0.75rem;
                margin-bottom: 1.5rem;
            }
            .print-button,
            .back-link,
            .print-link {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 0.7rem 1rem;
                border-radius: 0.75rem;
                border: 1px solid #d1d5db;
                background: #ffffff;
                color: #111827;
                text-decoration: none;
                font-weight: 600;
            }
            .print-button {
                cursor: pointer;
            }
            .print-sheet {
                border: 1px solid #d1d5db;
                border-radius: 1rem;
                background: #ffffff;
                padding: 1.5rem;
            }
            .print-title {
                margin: 0 0 1rem 0;
                font-size: 1.8rem;
            }
            .print-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 1rem;
            }
            .print-card {
                border: 1px solid #e5e7eb;
                border-radius: 0.9rem;
                padding: 1rem;
                break-inside: avoid;
            }
            .print-card.full {
                grid-column: 1 / -1;
            }
            .print-label {
                margin: 0 0 0.4rem 0;
                color: #6b7280;
                font-size: 0.85rem;
                text-transform: uppercase;
                letter-spacing: 0.04em;
            }
            .print-value {
                margin: 0;
                color: #111827;
                font-size: 1.05rem;
                white-space: pre-wrap;
                word-break: break-word;
            }
            @media print {
                .print-actions {
                    display: none !important;
                }
                .block-container {
                    max-width: 100%;
                    padding: 0;
                }
                .print-sheet {
                    border: none;
                    padding: 0;
                }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    registro = {coluna: st.query_params.get(coluna, "-") or "-" for _, coluna in COLUNAS_IMPRESSAO}
    titulo = (
        f'{registro["nome"]} | CPF {registro["cpf"]} | '
        f'Ficha {registro["numero_ficha"]} | Pasta {registro["pasta"]}'
    )

    cards_html = []
    for titulo_card, coluna in COLUNAS_IMPRESSAO:
        classe = "print-card full" if coluna == "endereco" else "print-card"
        cards_html.append(
            (
                f'<div class="{classe}">'
                f'<p class="print-label">{escape(titulo_card)}</p>'
                f'<p class="print-value">{escape(registro[coluna])}</p>'
                "</div>"
            )
        )

    acao_imprimir, acao_voltar = st.columns([1, 1])
    with acao_imprimir:
        if st.button("Imprimir", use_container_width=True):
            components.html(
                """
                <script>
                    const dispararImpressao = () => {
                        try {
                            window.parent.print();
                        } catch (e) {
                            window.print();
                        }
                    };
                    setTimeout(dispararImpressao, 150);
                </script>
                """,
                height=0,
                width=0,
            )
    with acao_voltar:
        st.button("Voltar para pesquisa", use_container_width=True, on_click=voltar_para_pesquisa)

    st.caption("Se o navegador nao abrir a impressao automaticamente, use Ctrl+P nessa tela.")

    st.markdown(
        (
            '<section class="print-sheet">'
            '<h1 class="print-title">Ficha para impressao</h1>'
            f"<p>{escape(titulo)}</p>"
            '<div class="print-grid">'
            f'{"".join(cards_html)}'
            "</div>"
            "</section>"
        ),
        unsafe_allow_html=True,
    )

    if st.query_params.get("auto_print") == "1":
        st.info("Pagina de impressao aberta. Se o dialogo nao aparecer, pressione Ctrl+P.")


def render_cards_registro(registro: pd.Series) -> None:
    grupos = [
        [
            ("Nome", registro["nome"]),
            ("CPF", registro["cpf"]),
            ("Cidade", registro["cidade"]),
        ],
        [
            ("Codigo", registro["codigo"]),
            ("Pasta", registro["pasta"]),
            ("Ficha", registro["numero_ficha"]),
        ],
        [
            ("Endereco", registro["endereco"]),
        ],
    ]

    for grupo in grupos:
        colunas = st.columns(len(grupo))
        for coluna, (titulo, valor) in zip(colunas, grupo):
            with coluna.container(border=True):
                cabecalho, acao = st.columns([5, 1])
                with cabecalho:
                    st.caption(f'{ICONES_CARDS.get(titulo, "•")} {titulo}')
                if titulo in {"Nome", "CPF"}:
                    with acao:
                        st.button(
                            "🔎",
                            key=f'pesquisar_{titulo}_{registro["codigo"]}_{registro["numero_ficha"]}',
                            help=f"Exportar {titulo.lower()} para a pesquisa",
                            on_click=exportar_para_pesquisa,
                            args=(valor,),
                        )
                st.write(valor_card(valor))

    render_botao_impressao_direta(registro)


def render_importacao() -> None:
    st.subheader("Importar planilha para o banco")
    st.write(
        "Escolha se a planilha sera enviada pelo navegador ou lida de um caminho local. "
        "A importacao usa a aba `DADOS`, normaliza o cabecalho e grava um SQLite pronto para pesquisa."
    )

    indice_modo_padrao = 0 if os.name == "nt" else 1
    modo_importacao = st.radio(
        "Origem da planilha",
        options=["Caminho local", "Enviar arquivo"],
        index=indice_modo_padrao,
        horizontal=True,
    )

    arquivo_selecionado = None

    if modo_importacao == "Caminho local":
        if os.name != "nt":
            st.info(
                "Se este app estiver rodando no Streamlit Cloud ou em Linux, um caminho `C:\\...` do seu PC "
                "nao sera acessivel. Nesses casos, use `Enviar arquivo`."
            )

        caminho_inicial = str(PLANILHA_PADRAO.parent if PLANILHA_PADRAO.exists() else BASE_DIR)
        caminho_informado = st.text_input(
            "Caminho da pasta ou arquivo",
            value=caminho_inicial,
            placeholder=r"Ex.: C:\Users\voce\Desktop\planilhas",
            help="Se informar uma pasta, o app lista os arquivos Excel encontrados nela.",
        )

        try:
            with st.spinner("Localizando planilhas..."):
                planilhas = listar_planilhas(caminho_informado)
        except Exception as erro:
            st.error(f"Falha ao ler o caminho informado: {erro}")
            return

        if not caminho_informado.strip():
            st.info("Informe um caminho para localizar as planilhas.")
            return

        if not planilhas:
            st.warning("Nenhum arquivo Excel encontrado no caminho informado.")
            return

        caminho_resolvido = resolver_caminho_informado(caminho_informado)
        if caminho_resolvido.is_file():
            arquivo_selecionado = planilhas[0]
            st.caption(f"Arquivo selecionado: {arquivo_selecionado}")
        else:
            indice_padrao = 0
            planilha_padrao_resolvida = PLANILHA_PADRAO.resolve() if PLANILHA_PADRAO.exists() else None
            if planilha_padrao_resolvida:
                planilhas_resolvidas = [arquivo.resolve() for arquivo in planilhas]
                if planilha_padrao_resolvida in planilhas_resolvidas:
                    indice_padrao = planilhas_resolvidas.index(planilha_padrao_resolvida)
            arquivo_selecionado = st.selectbox(
                "Arquivo encontrado",
                options=planilhas,
                index=indice_padrao,
                format_func=lambda arquivo: arquivo.name,
            )
            st.caption(f"Arquivo selecionado: {arquivo_selecionado}")
    else:
        arquivo_selecionado = st.file_uploader(
            "Selecione a planilha",
            type=["xlsm", "xlsx", "xls"],
        )
        if arquivo_selecionado is not None:
            st.caption(f"Arquivo selecionado: {arquivo_selecionado.name}")
        else:
            st.info("Envie uma planilha para importar.")
            return

    if st.button("Importar para o banco", type="primary", use_container_width=True):
        try:
            with st.spinner("Importando planilha e atualizando banco..."):
                df = carregar_planilha_excel(arquivo_selecionado)
                salvar_no_banco(df)
        except Exception as erro:
            st.error(f"Falha na importacao: {erro}")
        else:
            st.success(f"{len(df)} registros importados para {DB_PATH.name}.")
            st.dataframe(df[COLUNAS_EXIBICAO], use_container_width=True, hide_index=True)


def render_pesquisa() -> None:
    iniciar_estado()
    st.subheader("Pesquisa unica")
    total, atualizado = carregar_resumo_banco()

    if total == 0:
        st.info("Importe a planilha primeiro para habilitar a pesquisa.")
        return

    st.caption(f"Banco pronto com {total} registros. Ultima atualizacao: {atualizado}")

    with st.form("form_pesquisa"):
        st.text_input(
            "Digite nome ou CPF",
            key="termo_pesquisa",
            placeholder="Ex.: MARIA ou 12345678900",
        )
        pesquisar = st.form_submit_button("Pesquisar", use_container_width=True)

    if pesquisar:
        st.session_state["executar_pesquisa"] = True

    if not st.session_state["executar_pesquisa"]:
        return

    resultados = pesquisar_registros(st.session_state["termo_pesquisa"])
    if resultados.empty:
        st.warning("Nenhum registro encontrado.")
        return

    st.success(f"{len(resultados)} registro(s) encontrado(s).")
    for indice, registro in resultados.iterrows():
        titulo = (
            f'{registro["nome"]} | CPF {registro["cpf"]} | '
            f'Ficha {registro["numero_ficha"]} | Pasta {registro["pasta"]}'
        )
        with st.expander(titulo, expanded=indice == 0):
            render_cards_registro(registro)


def main() -> None:
    st.set_page_config(page_title="Fichas Inativas", layout="centered")

    if pagina_impressao_ativa():
        render_pagina_impressao()
        return

    st.title("Fichas Inativas")
    st.write("Importe a planilha para SQLite e pesquise por nome ou CPF retornando ficha e pasta.")

    aba_importacao, aba_pesquisa = st.tabs(["Importacao", "Pesquisa"])
    with aba_importacao:
        render_importacao()
    with aba_pesquisa:
        render_pesquisa()


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional


SECTION_ALIASES = {
    "execucao": "execucao",
    "execucao diaria": "execucao",
    "frentes": "frentes",
    "ocorrencias": "ocorrencias",
    "obs": "obs",
    "observacao": "obs",
    "observacoes": "obs",
}

FIELD_ALIASES = {
    "nucleo": "nucleo",
    "logradouro": "logradouro",
    "municipio": "municipio",
    "equipe": "equipe",
}


def remover_acentos(texto: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(ch))


def normalizar_texto(texto: str) -> str:
    if texto is None:
        return ""
    normalizado = remover_acentos(str(texto).lower())
    normalizado = normalizado.replace("\u2019", "'").replace("`", "'")
    normalizado = normalizado.replace("_", " ")
    normalizado = re.sub(r"[^\w\s]", " ", normalizado)
    normalizado = re.sub(r"\s+", " ", normalizado).strip()
    return normalizado


def normalizar_unidade(unidade: str) -> str:
    if not unidade:
        return ""
    unidade_bruta = str(unidade).strip().lower()
    unidade_base = normalizar_texto(unidade_bruta)
    if unidade_bruta in {"m\u00b2", "m2"}:
        return "m2"
    mapa = {
        "m": "m",
        "metro": "m",
        "metros": "m",
        "un": "un",
        "und": "un",
        "unidade": "un",
        "unidades": "un",
        "m2": "m2",
        "metro quadrado": "m2",
        "metros quadrados": "m2",
    }
    return mapa.get(unidade_base, unidade_base.replace(" ", ""))


def normalizar_quantidade(valor) -> Optional[float]:
    if valor in (None, ""):
        return None
    texto = str(valor).strip().replace(" ", "")
    if not texto:
        return None
    if "," in texto and "." in texto:
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    else:
        texto = texto.replace(",", ".")
    try:
        numero = float(texto)
    except ValueError:
        return None
    return float(int(numero)) if numero.is_integer() else round(numero, 3)


def extrair_primeira_equipe(valor: object) -> str:
    texto = str(valor or "").replace("\r\n", "\n").replace("\r", "\n")
    texto = re.sub(r"^[\u2022\-\*]+\s*", "", texto).strip()
    if not texto:
        return ""
    if texto.lower().startswith("multiplos") and ":" in texto:
        texto = texto.split(":", 1)[1].strip()

    partes = re.split(r"\s*(?:/|;|\||\n|,)\s*", texto)
    primeira = ""
    for parte in partes:
        limpa = " ".join(str(parte or "").split()).strip(" -")
        if limpa:
            primeira = limpa
            break
    if not primeira:
        primeira = " ".join(texto.split()).strip(" -")

    primeira = re.sub(
        r"^(?:resp(?:onsavel)?\.?|responsavel(?:\s+tecnico)?|equipes)\s*[:\-]?\s*",
        "",
        primeira,
        flags=re.IGNORECASE,
    ).strip(" -")
    if not primeira:
        return ""

    if re.fullmatch(r"\d{1,3}", primeira):
        return f"Equipe {primeira}"
    return primeira


def aplicar_regra_primeira_equipe(parsed: dict) -> dict:
    parsed["equipe"] = extrair_primeira_equipe(parsed.get("equipe", ""))
    for bucket in ("frentes", "execucao", "ocorrencias", "observacoes", "servicos_nao_mapeados"):
        for row in parsed.get(bucket, []):
            if isinstance(row, dict):
                row["equipe"] = extrair_primeira_equipe(row.get("equipe", ""))
    return parsed


def _linha_item(linha: str) -> str:
    return re.sub(r"^\s*[-\u2022*]\s*", "", linha).strip()


def _canonical_field(label: str) -> Optional[str]:
    return FIELD_ALIASES.get(normalizar_texto(label))


def _canonical_section(label: str) -> Optional[str]:
    return SECTION_ALIASES.get(normalizar_texto(label))


def extrair_blocos_mensagem(texto: str) -> Dict[str, object]:
    resultado: Dict[str, object] = {
        "data": "",
        "nucleo": "",
        "logradouro": "",
        "municipio": "",
        "equipe": "",
        "execucao": [],
        "frentes": [],
        "ocorrencias": [],
        "obs": [],
    }
    linhas = [ln.rstrip() for ln in texto.splitlines()]
    secoes_detectadas = set()
    campos_detectados = set()
    secao_atual: Optional[str] = None

    for linha in linhas:
        linha_limpa = linha.strip()
        if not linha_limpa:
            continue

        if not resultado["data"]:
            m_data = re.search(r"\b\d{2}/\d{2}/\d{4}\b", linha_limpa)
            if m_data:
                resultado["data"] = m_data.group(0)

        if ":" in linha_limpa:
            rotulo, valor = linha_limpa.split(":", 1)
            rotulo = rotulo.strip()
            valor = valor.strip()
            campo = _canonical_field(rotulo)
            if campo:
                resultado[campo] = valor
                campos_detectados.add(campo)
                secao_atual = None
                continue

            secao = _canonical_section(rotulo)
            if secao:
                secao_atual = secao
                secoes_detectadas.add(secao)
                if valor:
                    resultado[secao].append(_linha_item(valor))
                continue

        secao_sem_dois_pontos = _canonical_section(linha_limpa.rstrip(":"))
        if secao_sem_dois_pontos and linha_limpa.endswith(":"):
            secao_atual = secao_sem_dois_pontos
            secoes_detectadas.add(secao_sem_dois_pontos)
            continue

        if secao_atual:
            resultado[secao_atual].append(_linha_item(linha_limpa))

    resultado["modelo_oficial"] = bool(
        "execucao" in secoes_detectadas and bool({"logradouro", "equipe", "municipio"} & campos_detectados)
    )
    return resultado


def parsear_linha_execucao(linha: str) -> Dict[str, object]:
    original = linha.strip()
    texto = _linha_item(original)
    quantidade = None
    unidade = ""
    servico = texto

    m_qtd = re.match(r"^\s*(\d+(?:[.,]\d+)?)(.*)$", texto)
    if m_qtd and m_qtd.group(2).strip():
        quantidade = normalizar_quantidade(m_qtd.group(1))
        resto = m_qtd.group(2).strip()
        m_unidade = re.match(
            r"^(m\u00b2|m2|metro quadrado|metros quadrados|metro|metros|m|unidades|unidade|und|un)\b\s*(.*)$",
            resto,
            flags=re.IGNORECASE,
        )
        if m_unidade:
            unidade = normalizar_unidade(m_unidade.group(1))
            servico = m_unidade.group(2).strip() or resto
        else:
            servico = resto

    servico = servico.strip(" -:;,." )
    if not servico:
        servico = texto

    # Regra operacional: quando vier "servico complementar: ...",
    # usa o texto apos os dois pontos para mapear o servico real executado.
    servico_normalizado_base = normalizar_texto(servico)
    if servico_normalizado_base.startswith("servico complementar") and ":" in servico:
        _, detalhe = servico.split(":", 1)
        detalhe = detalhe.strip(" -:;,." )
        if detalhe:
            servico = detalhe

    return {
        "mensagem_original": original,
        "quantidade": quantidade,
        "unidade": normalizar_unidade(unidade) if unidade else "",
        "servico_bruto": servico,
        "servico_normalizado": normalizar_texto(servico),
    }


class ServiceDictionaryV2:
    def __init__(self, json_path: Path):
        self.entries: List[dict] = []
        self.alias_index: Dict[str, dict] = {}
        self.service_index: Dict[str, dict] = {}
        self._load(json_path)

    def _load(self, json_path: Path) -> None:
        data = json.loads(Path(json_path).read_text(encoding="utf-8-sig"))
        for raw in data:
            entry = dict(raw)
            entry["unidades_aceitas"] = [normalizar_unidade(u) for u in entry.get("unidades_aceitas", []) if u]
            aliases = set(entry.get("aliases", []))
            aliases.add(entry.get("servico_oficial", ""))
            aliases_normalizados = sorted({normalizar_texto(alias) for alias in aliases if alias})
            entry["aliases_normalizados"] = aliases_normalizados
            self.entries.append(entry)
            servico = entry.get("servico_oficial", "")
            if servico:
                self.service_index[servico] = entry
            for alias in aliases_normalizados:
                self.alias_index.setdefault(alias, entry)

    def _build_match(self, entry: dict, regra: str) -> dict:
        servico_oficial = entry.get("servico_oficial", "servico_nao_mapeado")
        categoria = entry.get("categoria", "servico_nao_mapeado")
        if servico_oficial == "hidrometro":
            categoria = "hidrometro"
        return {
            "servico_oficial": servico_oficial,
            "categoria": categoria,
            "unidades_aceitas": entry.get("unidades_aceitas", []),
            "regra_disparada": regra,
            "sugestao_categoria": "",
        }

    def _contains_controlado(self, servico_normalizado: str) -> Optional[dict]:
        tokens_servico = set(servico_normalizado.split())
        melhor = None
        melhor_score = 0
        for entry in self.entries:
            for alias in entry.get("aliases_normalizados", []):
                if len(alias) < 5:
                    continue
                if alias in servico_normalizado or servico_normalizado in alias:
                    tokens_alias = set(alias.split())
                    if not tokens_alias:
                        continue
                    taxa_cobertura = len(tokens_alias & tokens_servico) / len(tokens_alias)
                    if taxa_cobertura < 0.6 and len(tokens_alias) > 1:
                        continue
                    score = len(alias)
                    if score > melhor_score:
                        melhor = entry
                        melhor_score = score
        return melhor

    def _keyword_match(self, servico_normalizado: str) -> Optional[dict]:
        tokens = servico_normalizado.split()

        def has_token_prefix(prefix: str) -> bool:
            return any(t.startswith(prefix) for t in tokens)

        def has_token(token: str) -> bool:
            return token in tokens

        regras = [
            ("hidrometro", "keyword_hidromet", lambda s: "hidromet" in s),
            ("caixa_uma", "keyword_caixa_uma", lambda s: "embutid" in s or "mureta" in s),
            ("caixa_inspecao", "keyword_caixa_inspecao", lambda s: "caixa de inspec" in s or "poco de inspec" in s or has_token("pi")),
            ("cavalete", "keyword_cavalet", lambda s: "cavalet" in s),
            ("reparo_vazamento", "keyword_vazament", lambda s: "vazament" in s),
            ("recomposicao_passeio", "keyword_calcad", lambda s: "calcad" in s or "passeio" in s),
            ("recomposicao_asfalto", "keyword_asfalt", lambda s: "asfalt" in s),
            ("rede_esgoto", "keyword_rede_esgoto", lambda s: "esgot" in s and has_token_prefix("rede")),
            ("rede_agua", "keyword_rede", lambda s: has_token_prefix("rede")),
            ("ligacao_agua", "keyword_liga", lambda s: has_token_prefix("liga")),
            ("ramal", "keyword_ramal", lambda s: "ramal" in s),
            ("vala", "keyword_vala", lambda s: "vala" in s),
            ("reaterro", "keyword_reaterro", lambda s: "reaterro" in s or "compact" in s),
        ]
        for servico, regra, cond in regras:
            if cond(servico_normalizado):
                entry = self.service_index.get(servico, {})
                if not entry:
                    entry = {
                        "servico_oficial": servico,
                        "categoria": "hidrometro" if servico == "hidrometro" else servico,
                        "unidades_aceitas": [],
                    }
                return self._build_match(entry, regra)
        return None
    def mapear_servico(self, servico_bruto: str) -> dict:
        servico_normalizado = normalizar_texto(servico_bruto)
        if not servico_normalizado:
            return {
                "servico_oficial": "servico_nao_mapeado",
                "categoria": "servico_nao_mapeado",
                "unidades_aceitas": [],
                "regra_disparada": "nao_mapeado",
                "sugestao_categoria": "",
            }

        exact = self.alias_index.get(servico_normalizado)
        if exact:
            return self._build_match(exact, "alias_exato")

        contains = self._contains_controlado(servico_normalizado)
        if contains:
            return self._build_match(contains, "contains_controlado")

        keyword = self._keyword_match(servico_normalizado)
        if keyword:
            return keyword

        return {
            "servico_oficial": "servico_nao_mapeado",
            "categoria": "servico_nao_mapeado",
            "unidades_aceitas": [],
            "regra_disparada": "nao_mapeado",
            "sugestao_categoria": "",
        }


def carregar_dicionario_servicos(caminho: Path) -> ServiceDictionaryV2:
    return ServiceDictionaryV2(caminho)


def mapear_servico(servico_bruto: str, dicionario: ServiceDictionaryV2) -> dict:
    return dicionario.mapear_servico(servico_bruto)


def registrar_servico_nao_mapeado(
    registros: List[dict],
    contexto: dict,
    execucao_item: dict,
    mapeamento: dict,
) -> None:
    registros.append(
        {
            "data": contexto.get("data", ""),
            "mensagem_original": execucao_item.get("mensagem_origem", ""),
            "servico_bruto": execucao_item.get("servico_bruto", ""),
            "servico_normalizado": execucao_item.get("servico_normalizado", ""),
            "quantidade": execucao_item.get("quantidade", ""),
            "unidade": execucao_item.get("unidade", ""),
            "nucleo": contexto.get("nucleo", ""),
            "logradouro": contexto.get("logradouro", ""),
            "municipio": contexto.get("municipio", ""),
            "equipe": contexto.get("equipe", ""),
            "sugestao_categoria": mapeamento.get("sugestao_categoria", ""),
            "regra_disparada": mapeamento.get("regra_disparada", "nao_mapeado"),
        }
    )


class OfficialMessageParser:
    def __init__(
        self,
        service_dictionary: ServiceDictionaryV2,
        contrato_padrao: str = "Oeste 1",
        programa_padrao: str = "Agua Legal",
    ):
        self.service_dictionary = service_dictionary
        self.contrato_padrao = contrato_padrao
        self.programa_padrao = programa_padrao

    def _classificar_ocorrencia(self, descricao: str) -> str:
        d = normalizar_texto(descricao)
        if any(k in d for k in ["chuva", "tempestade", "temporal"]):
            return "clima"
        if "vistoria" in d:
            return "vistoria"
        if any(k in d for k in ["vazamento", "reparo"]):
            return "reparo"
        if any(k in d for k in ["interferencia", "interferencia com rede", "esgoto"]):
            return "interferencia"
        if any(k in d for k in ["material", "equipamento", "compressor", "espera"]):
            return "restricao_operacional"
        return "ocorrencia_operacional"

    def _impacto_ocorrencia(self, descricao: str) -> str:
        d = normalizar_texto(descricao)
        if any(k in d for k in ["interromp", "sem producao", "impossibil", "nao foi possivel"]):
            return "sim"
        if any(k in d for k in ["parcial", "impact", "atras"]):
            return "parcial"
        return "parcial"

    def parse_text(self, texto: str, source_name: str = "mensagem_whatsapp.txt") -> Optional[dict]:
        blocos = extrair_blocos_mensagem(texto)
        if not blocos.get("modelo_oficial"):
            return None

        data = str(blocos.get("data", "") or "")
        nucleo = str(blocos.get("nucleo", "") or "")
        logradouro = str(blocos.get("logradouro", "") or "")
        municipio = str(blocos.get("municipio", "") or "")
        equipe = extrair_primeira_equipe(blocos.get("equipe", ""))
        contrato = self.contrato_padrao
        programa = self.programa_padrao

        frentes_raw: List[str] = list(blocos.get("frentes", []))
        if not frentes_raw:
            frentes_raw = [""]

        frentes: List[dict] = []
        status_frente = "com_producao" if blocos.get("execucao") else "sem_producao"
        for idx, frente in enumerate(frentes_raw, start=1):
            frentes.append(
                {
                    "id_frente": f"F{idx:03d}",
                    "data_referencia": data,
                    "data": data,
                    "contrato": contrato,
                    "programa": programa,
                    "nucleo": nucleo,
                    "equipe": equipe,
                    "logradouro": logradouro,
                    "municipio": municipio,
                    "status_frente": status_frente,
                    "observacao_frente": frente.strip(),
                    "frente": frente.strip(),
                    "arquivo_origem": source_name,
                }
            )

        id_frente_principal = frentes[0]["id_frente"]
        execucao: List[dict] = []
        servicos_nao_mapeados: List[dict] = []

        for idx, linha in enumerate(list(blocos.get("execucao", [])), start=1):
            parsed = parsear_linha_execucao(str(linha))
            mapeamento = self.service_dictionary.mapear_servico(str(parsed["servico_bruto"]))
            quantidade = parsed["quantidade"] if parsed["quantidade"] is not None else ""
            unidade = str(parsed["unidade"] or "")
            if not unidade and mapeamento.get("unidades_aceitas") and quantidade not in ("", None):
                unidade = mapeamento["unidades_aceitas"][0]
            unidade = normalizar_unidade(unidade) if unidade else ""
            mensagem_origem = str(parsed.get("mensagem_original", "")).strip()
            if mensagem_origem and not mensagem_origem.startswith(("-", "\u2022", "*")):
                mensagem_origem = f"- {mensagem_origem}"

            exec_item = {
                "id_item": f"I{idx:04d}",
                "id_frente": id_frente_principal,
                "data_referencia": data,
                "data": data,
                "contrato": contrato,
                "programa": programa,
                "nucleo": nucleo,
                "logradouro": logradouro,
                "municipio": municipio,
                "equipe": equipe,
                "item_original": parsed["servico_bruto"],
                "item_normalizado": mapeamento["servico_oficial"],
                "categoria_item": mapeamento["categoria"],
                "tipo_registro": "servico",
                "quantidade": quantidade,
                "unidade": unidade,
                "material": "",
                "especificacao": "",
                "complemento": "",
                "observacao_item": "",
                "arquivo_origem": source_name,
                "mensagem_origem": mensagem_origem,
                "servico_bruto": parsed["servico_bruto"],
                "servico_normalizado": parsed["servico_normalizado"],
                "servico_oficial": mapeamento["servico_oficial"],
                "categoria": mapeamento["categoria"],
                "regra_disparada": mapeamento["regra_disparada"],
            }
            execucao.append(exec_item)

            if mapeamento["servico_oficial"] == "servico_nao_mapeado":
                registrar_servico_nao_mapeado(
                    registros=servicos_nao_mapeados,
                    contexto={
                        "data": data,
                        "nucleo": nucleo,
                        "logradouro": logradouro,
                        "municipio": municipio,
                        "equipe": equipe,
                    },
                    execucao_item=exec_item,
                    mapeamento=mapeamento,
                )

        ocorrencias: List[dict] = []
        occ_counter = 0
        for item in list(blocos.get("ocorrencias", [])):
            descricao = str(item).strip()
            if not descricao:
                continue
            occ_counter += 1
            ocorrencias.append(
                {
                    "id_ocorrencia": f"O{occ_counter:04d}",
                    "id_frente": id_frente_principal,
                    "data_referencia": data,
                    "data": data,
                    "contrato": contrato,
                    "programa": programa,
                    "nucleo": nucleo,
                    "logradouro": logradouro,
                    "municipio": municipio,
                    "equipe": equipe,
                    "tipo_ocorrencia": self._classificar_ocorrencia(descricao),
                    "descricao": descricao,
                    "impacto_producao": self._impacto_ocorrencia(descricao),
                    "arquivo_origem": source_name,
                }
            )

        observacoes: List[dict] = []
        for obs in list(blocos.get("obs", [])):
            descricao = str(obs).strip()
            if not descricao:
                continue
            observacoes.append(
                {
                    "data": data,
                    "nucleo": nucleo,
                    "logradouro": logradouro,
                    "municipio": municipio,
                    "equipe": equipe,
                    "observacao": descricao,
                    "arquivo_origem": source_name,
                }
            )
            occ_counter += 1
            ocorrencias.append(
                {
                    "id_ocorrencia": f"O{occ_counter:04d}",
                    "id_frente": id_frente_principal,
                    "data_referencia": data,
                    "data": data,
                    "contrato": contrato,
                    "programa": programa,
                    "nucleo": nucleo,
                    "logradouro": logradouro,
                    "municipio": municipio,
                    "equipe": equipe,
                    "tipo_ocorrencia": "observacao_geral",
                    "descricao": descricao,
                    "impacto_producao": "parcial",
                    "arquivo_origem": source_name,
                }
            )

        return {
            "data_referencia": data,
            "contrato": contrato,
            "programa": programa,
            "arquivo_origem": source_name,
            "frentes": frentes,
            "execucao": execucao,
            "ocorrencias": ocorrencias,
            "observacoes": observacoes,
            "servicos_nao_mapeados": servicos_nao_mapeados,
        }

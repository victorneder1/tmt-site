from __future__ import annotations

import json
import smtplib
from email.message import EmailMessage
from urllib.request import Request, urlopen

from .config import AppConfig


class TeamsNotifier:
    def __init__(self, config: AppConfig) -> None:
        self.webhook_url = config.teams_webhook_url

    @property
    def is_enabled(self) -> bool:
        return bool(self.webhook_url)

    def notify_documents(self, documents: list[dict[str, str]]) -> list[str]:
        if not self.is_enabled or not documents:
            return []

        # powerplatform.com flows expect Adaptive Card format (same as classic connector)
        # logic.azure.com simple-trigger flows expect plain {"text": "..."}
        url = self.webhook_url or ""
        is_simple_text = "logic.azure.com" in url and "powerplatform.com" not in url

        delivered: list[str] = []
        for document in documents:
            payload = (
                build_workflows_payload(document)
                if is_simple_text
                else build_teams_payload(document)
            )
            body = json.dumps(payload).encode("utf-8")
            request = Request(
                self.webhook_url,
                data=body,
                headers={"Content-Type": "application/json; charset=utf-8"},
                method="POST",
            )
            with urlopen(request, timeout=30) as response:
                if response.status < 200 or response.status >= 300:
                    raise RuntimeError(f"Teams respondeu com status {response.status}.")
            delivered.append(document["Protocolo_Entrega"])

        return delivered


class EmailNotifier:
    def __init__(self, config: AppConfig) -> None:
        self.smtp = config.smtp

    @property
    def is_enabled(self) -> bool:
        return self.smtp.is_enabled

    def notify_documents(self, documents: list[dict[str, str]]) -> list[str]:
        if not self.is_enabled or not documents:
            return []

        delivered: list[str] = []
        use_ssl = self.smtp.port == 465
        if use_ssl:
            conn = smtplib.SMTP_SSL(self.smtp.host, self.smtp.port, timeout=30)
        else:
            conn = smtplib.SMTP(self.smtp.host, self.smtp.port, timeout=30)
            conn.ehlo()
            conn.starttls()
            conn.ehlo()
        with conn:
            if self.smtp.user and self.smtp.password:
                conn.login(self.smtp.user, self.smtp.password)
            for document in documents:
                message = build_email_message(self.smtp.from_email or "", self.smtp.to_emails, document)
                conn.send_message(message)
                delivered.append(document["Protocolo_Entrega"])
        return delivered


class CompositeNotifier:
    def __init__(self, config: AppConfig) -> None:
        self.notifiers = [TeamsNotifier(config), EmailNotifier(config)]

    @property
    def is_enabled(self) -> bool:
        return any(notifier.is_enabled for notifier in self.notifiers)

    def notify_documents(self, documents: list[dict[str, str]]) -> list[str]:
        delivered_sets: list[set[str]] = []
        for notifier in self.notifiers:
            delivered_sets.append(set(notifier.notify_documents(documents)))
        if not delivered_sets:
            return []
        merged = set().union(*delivered_sets)
        return sorted(merged)


def build_workflows_payload(document: dict[str, str]) -> dict[str, object]:
    """Payload for the Teams Workflows 'Post to channel when webhook received' template."""
    company = document.get("company_alias") or document.get("Nome_Companhia") or "Companhia"
    document_kind = document.get("document_kind") or "-"
    reference_date = document.get("Data_Referencia") or "-"
    delivery_date = document.get("Data_Entrega") or "-"
    protocol = document.get("Protocolo_Entrega") or "-"
    parse_status = document.get("parse_status") or "-"
    cvm_code = document.get("Codigo_CVM", "").strip() or "-"

    lines = [
        f"**New CVM 358 filing — {company}**",
        f"Type: {document_kind} | Reference: {reference_date} | Filed: {delivery_date}",
        f"CVM code: {cvm_code} | Protocol: {protocol} | Parse: {parse_status}",
        f"[Open document (login required)]({_cvm_direct_url(document)})  |  [Search on CVM ENET]({_cvm_search_url()})",
    ]
    return {"text": "\n\n".join(lines)}


ENET_SEARCH_URL = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"


def _cvm_direct_url(document: dict[str, str]) -> str:
    """Direct download link — opens the document if the user is logged in to CVM."""
    return document.get("Link_Download") or ENET_SEARCH_URL


def _cvm_search_url() -> str:
    """CVM ENET search page — no login required."""
    return ENET_SEARCH_URL


def build_teams_payload(document: dict[str, str]) -> dict[str, object]:
    company = document.get("company_alias") or document.get("Nome_Companhia") or "Companhia"
    category = document.get("Categoria") or "Documento CVM"
    subject = document.get("Assunto") or "Sem assunto informado."
    delivery_date = document.get("Data_Entrega") or "-"
    reference_date = document.get("Data_Referencia") or "-"
    version = document.get("Versao") or "-"
    protocol = document.get("Protocolo_Entrega") or "-"
    document_kind = document.get("document_kind") or "-"
    parse_status = document.get("parse_status") or "-"
    cvm_code = document.get("Codigo_CVM", "").strip() or "-"
    direct_url = _cvm_direct_url(document)
    search_url = _cvm_search_url()

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"New CVM 358 filing: {company}",
                            "weight": "Bolder",
                            "size": "Large",
                            "wrap": True,
                        },
                        {
                            "type": "TextBlock",
                            "text": f"{category} | {document_kind}",
                            "spacing": "Small",
                            "wrap": True,
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Reference date", "value": reference_date},
                                {"title": "Filing date",    "value": delivery_date},
                                {"title": "CVM code",       "value": cvm_code},
                                {"title": "Protocol",       "value": protocol},
                                {"title": "Version",        "value": version},
                                {"title": "Parse status",   "value": parse_status},
                            ],
                        },
                        {
                            "type": "TextBlock",
                            "text": subject,
                            "wrap": True,
                            "isSubtle": True,
                        },
                    ],
                    "actions": [
                        {
                            "type": "Action.OpenUrl",
                            "title": "Open document (CVM login required)",
                            "url": direct_url,
                        },
                        {
                            "type": "Action.OpenUrl",
                            "title": "Search on CVM ENET",
                            "url": search_url,
                        },
                    ],
                },
            }
        ],
    }


def build_email_message(
    from_email: str,
    to_emails: list[str],
    document: dict[str, str],
) -> EmailMessage:
    company = document.get("company_alias") or document.get("Nome_Companhia") or "Companhia"
    protocol = document.get("Protocolo_Entrega") or "-"
    parse_status = document.get("parse_status") or "-"
    cvm_code = document.get("Codigo_CVM", "").strip() or "-"
    message = EmailMessage()
    message["Subject"] = f"[CVM 358] {company} | {document.get('document_kind', '-')}"
    message["From"] = from_email
    message["To"] = ", ".join(to_emails)
    message.set_content(
        "\n".join(
            [
                f"Company:        {company}",
                f"Type:           {document.get('document_kind', '-')}",
                f"Reference date: {document.get('Data_Referencia', '-')}",
                f"Filing date:    {document.get('Data_Entrega', '-')}",
                f"CVM code:       {cvm_code}",
                f"Protocol:       {protocol}",
                f"Parse status:   {parse_status}",
                f"",
                f"Open document (login required):",
                f"  {_cvm_direct_url(document)}",
                f"",
                f"Search on CVM ENET (no login):",
                f"  {_cvm_search_url()}",
            ]
        )
    )
    return message

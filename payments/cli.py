from __future__ import annotations

import argparse
import json
import sys

from .config import ROOT, load_env, runtime, write_json


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Desafio: pipeline de pagamentos")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("setup", "teardown"):
        command = commands.add_parser(name)
        command.add_argument("--apply", action="store_true", help="Executa alteracoes reais")
    commands.add_parser("check", help="Valida seed e contratos localmente, sem acesso a nuvem")
    commands.add_parser("demo-local", help="Referencia local com Avro e SQLite; nao valida a nuvem")
    commands.add_parser("demo", help="Gera e captura eventos CDC e alerta reais")
    commands.add_parser("evidence", help="Coleta seguranca, metricas e custos reais")
    commands.add_parser("billing", help="Reconsulta custos, inclusive apos teardown")
    consumer = commands.add_parser("consume")
    consumer.add_argument("--seconds", type=int, default=None)
    args = parser.parse_args(argv)
    config = load_env()
    try:
        if args.command in ("setup", "teardown"):
            from . import provision

            getattr(provision, args.command)(config, apply=args.apply)
        elif args.command == "check":
            from .contracts import local_compatibility
            from .seed import COUNTS, dataset

            data = dataset()
            counts = {name: len(rows) for name, rows in data.items()}
            compatibility = local_compatibility()
            if counts != COUNTS or any(c["compatible"] != c["expected"] for c in compatibility):
                raise ValueError("Verificacao local falhou")
            result = {"mode": "local-only", "counts": counts, "compatibility": compatibility}
            write_json(ROOT / "output/local-check.json", result)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.command == "demo-local":
            from .local_demo import run

            result = run()
            print(
                f"Demonstracao local: {result['enriched_rows']} transacoes enriquecidas, "
                f"{len(result['alerts'])} alerta(s). Resultado em output/local-demo.json"
            )
        elif args.command == "consume":
            from .consumer import consume

            if args.seconds is not None and args.seconds <= 0:
                raise ValueError("--seconds deve ser positivo")
            consume(runtime(), config.get("CONSUMER_GROUP", "desafio-alerts"), args.seconds)
        elif args.command == "demo":
            from .evidence import capture_cdc, capture_fraud

            state = runtime()
            capture_cdc(state, config)
            capture_fraud(state, config)
            print("Envelopes CDC e alerta real capturados em evidencias/.")
        elif args.command == "evidence":
            from . import evidence

            state = runtime()
            failures = []
            for name in ("security", "metrics", "billing"):
                try:
                    getattr(evidence, name)(state, config)
                except Exception as exc:
                    failures.append(name)
                    print(
                        f"Coleta {name} falhou: {type(exc).__name__}. "
                        "Confira permissoes e conectividade.",
                        file=sys.stderr,
                    )
            if failures:
                raise RuntimeError("Coletas pendentes: " + ", ".join(failures))
        elif args.command == "billing":
            from .evidence import billing

            context = ROOT / ".state/billing-context.json"
            state = (
                runtime()
                if (ROOT / ".state/runtime.json").exists()
                else json.loads(context.read_text(encoding="utf-8"))
            )
            billing(state, config)
    except KeyboardInterrupt:
        print("Interrompido. Recursos de nuvem permanecem ate teardown --apply.", file=sys.stderr)
        return 130
    except Exception as exc:
        from .provision import scrub

        # Nao exibe traceback com locais/argumentos que possam conter credenciais.
        print(scrub(f"{type(exc).__name__}: {exc}", config), file=sys.stderr)
        return 1
    return 0

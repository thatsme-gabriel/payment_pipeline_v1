"""Gera uma imagem do mapa mental. Opcional: requer Pillow e uma fonte TrueType."""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
WIDTH, HEIGHT = 1920, 1180
BACKGROUND = "#f5f7fb"
INK = "#183248"
MUTED = "#526677"
LINE = "#b4c5d3"
BLUE = "#12689a"
FONT_CANDIDATES = [
    Path("C:/Windows/Fonts/segoeui.ttf"),
    Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
]
FONT = next((path for path in FONT_CANDIDATES if path.exists()), None)
if FONT is None:
    raise SystemExit("Disponibilize Segoe UI ou DejaVu Sans para renderizar o mapa")


def font(size):
    return ImageFont.truetype(str(FONT), size)


def multiline(draw, xy, lines, size=25, fill=INK, spacing=38):
    x, y = xy
    for line in lines:
        draw.text((x, y), line, font=font(size), fill=fill)
        y += spacing


def render():
    image = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND)
    draw = ImageDraw.Draw(image)
    draw.text((100, 55), "Do pagamento ao alerta", font=font(56), fill=INK)
    draw.text(
        (102, 137),
        "Como o banco, os recados e a análise trabalham juntos",
        font=font(28),
        fill=MUTED,
    )
    center = (730, 460, 1190, 700)
    branches = [
        (
            (100, 240, 630, 420),
            "01  O banco guarda os fatos",
            [
                "Neon: o caderno da operação.",
                "Guarda contas, clientes e compras.",
                "Exemplo: uma compra de R$ 19,90.",
            ],
            "#e4f0f7",
        ),
        (
            (100, 490, 630, 670),
            "02  Um observador avisa",
            [
                "Debezium percebe cada novidade.",
                "O que entrou, mudou ou saiu vira aviso.",
                "Sem redigitar a informação.",
            ],
            "#e8f2ed",
        ),
        (
            (100, 740, 630, 920),
            "03  Os recados seguem caminho",
            [
                "Kafka organiza os avisos por assunto.",
                "Guarda o que chegou por um tempo.",
                "Cada aplicação acompanha no seu ritmo.",
            ],
            "#f8efdf",
        ),
        (
            (1290, 240, 1820, 420),
            "04  Todos usam o mesmo formato",
            [
                "Um combinado diz o que cada aviso traz.",
                "Ajuda a mudar sem quebrar os leitores.",
                "Identifica informações sensíveis.",
            ],
            "#ece8f5",
        ),
        (
            (1290, 490, 1820, 670),
            "05  A análise encontra sinais",
            [
                "Flink junta a compra aos dados da conta.",
                "Três compras rápidas no mesmo cartão?",
                "É hora de preparar um alerta.",
            ],
            "#e1f0ee",
        ),
        (
            (1290, 740, 1820, 920),
            "06  Uma aplicação recebe",
            [
                "Lê o alerta quando ele chega.",
                "Registra uma única vez, mesmo repetido.",
                "O alerta ajuda alguém a verificar.",
            ],
            "#f5e7e3",
        ),
    ]
    for box, title, lines, fill in branches:
        left, top, right, bottom = box
        target = (right, (top + bottom) // 2) if left < 900 else (left, (top + bottom) // 2)
        source = (center[0], 580) if left < 900 else (center[2], 580)
        draw.line([source, target], fill=LINE, width=4)
        draw.rounded_rectangle(box, radius=22, fill=fill)
        draw.text((left + 26, top + 21), title, font=font(27), fill=INK)
        multiline(draw, (left + 26, top + 72), lines, size=23, spacing=32, fill=MUTED)
    draw.rounded_rectangle(center, radius=30, fill=INK)
    draw.text((960, 514), "Confluent Cloud", font=font(39), fill="white", anchor="mt")
    draw.text((960, 582), "A central que conecta", font=font(27), fill="#e0edf5", anchor="mt")
    draw.text((960, 624), "e acompanha o caminho", font=font(27), fill="#e0edf5", anchor="mt")
    draw.rounded_rectangle((100, 990, 1820, 1118), radius=20, fill="#e7edf4")
    draw.text((130, 1010), "O cuidado acompanha tudo", font=font(27), fill=BLUE)
    draw.text(
        (130, 1057),
        "Chaves de acesso  •  Acompanhamento de atrasos e gastos  •  AWS: a estrutura por baixo",
        font=font(25),
        fill=MUTED,
    )
    path = ROOT / "docs/mapa-mental.png"
    path.parent.mkdir(exist_ok=True)
    image.save(path, optimize=True)
    print(path)


if __name__ == "__main__":
    render()

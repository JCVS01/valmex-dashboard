"""Genera PDF con tabla de validacion de ISINs vs API Morningstar NAV."""
from fpdf import FPDF

class SeriesPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 8, "Validacion de Series - Morningstar NAV API", align="C", new_x="LMARGIN", new_y="NEXT")
        self.set_font("Helvetica", "", 8)
        self.cell(0, 5, "Generado: 2026-03-03  |  API: UnadjustedNAV por ISIN  |  Rango consulta: 2000-01-01 a 2026-03-03", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "I", 7)
        self.cell(0, 10, f"Pagina {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 10)
        self.set_fill_color(41, 65, 122)
        self.set_text_color(255, 255, 255)
        self.cell(0, 7, f"  {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)
        self.ln(1)

    def table_header(self, cols=None):
        self.set_font("Helvetica", "B", 7)
        self.set_fill_color(220, 225, 235)
        if cols is None:
            cols = [
                ("Fondo", 20), ("Serie", 12), ("Tipo Cliente", 18),
                ("ISIN", 32), ("Nombre API", 28),
                ("Desde", 20), ("Hasta", 20), ("# Precios", 16), ("Notas", 30),
            ]
        for label, w in cols:
            self.cell(w, 5, label, border=1, fill=True, align="C")
        self.ln()

    def table_row(self, row, shade=False):
        self.set_font("Helvetica", "", 6.5)
        if shade:
            self.set_fill_color(245, 245, 250)
        else:
            self.set_fill_color(255, 255, 255)

        cols = [
            (row.get("fondo", ""), 20),
            (row.get("serie", ""), 12),
            (row.get("tipo", ""), 18),
            (row.get("isin", ""), 32),
            (row.get("api_name", ""), 28),
            (row.get("desde", ""), 20),
            (row.get("hasta", ""), 20),
            (row.get("count", ""), 16),
            (row.get("notas", ""), 30),
        ]

        # Check if notes indicate a problem
        notas = row.get("notas", "")
        for label, w in cols:
            if label == notas and notas:
                self.set_text_color(180, 0, 0)
            self.cell(w, 4.5, str(label), border=1, fill=shade, align="C" if w <= 20 else "L")
            self.set_text_color(0, 0, 0)
        self.ln()


# -- Data from validation --
# Format: (fondo, serie, tipo_cliente, isin, api_name, desde, hasta, count, notas)
data = {
    "Deuda MXN": [
        ("VXREPO1", "A", "Serie A", "MXP800461008", "VXREPO1 A", "2000-11-01", "2026-03-02", "4,897", ""),
        ("VXREPO1", "B1FI", "Persona Fisica", "MX51VA2J00F8", "VXREPO1 B1FI", "2006-01-02", "2026-03-02", "5,066", ""),
        ("VXREPO1", "B0FI", "PF con Fee", "MX51VA2J0074", "VXREPO1 B0FI", "2006-01-02", "2026-03-02", "5,050", ""),
        ("VXREPO1", "B1CF", "PPR", "MX51VA2J00D3", "VXREPO1 B1CF", "2008-07-03", "2026-03-02", "4,235", ""),
        ("VXREPO1", "B1CO", "Persona Moral", "MX51VA2J0082", "VXREPO1 B1CO", "2008-07-04", "2026-03-02", "4,438", ""),
        ("VXREPO1", "B0CO", "PM con Fee", "MX51VA2J0058", "VXREPO1 B0CO", "2008-07-02", "2026-03-02", "4,440", ""),

        ("VXGUBCP", "A", "Serie A", "MXP800501001", "VXGUBCP A", "2000-11-01", "2026-03-02", "4,895", ""),
        ("VXGUBCP", "B1FI", "Persona Fisica", "MX51VA2L0062", "VXGUBCP B1FI", "2000-11-01", "2026-03-02", "6,343", ""),
        ("VXGUBCP", "B0FI", "PF con Fee", "MX51VA2L0039", "VXGUBCP B0FI", "2005-09-06", "2026-03-02", "5,142", ""),
        ("VXGUBCP", "B1CF", "PPR", "MX51VA2L00C1", "VXGUBCP B1CF", "2008-07-04", "2026-03-02", "4,231", ""),
        ("VXGUBCP", "B1CO", "Persona Moral", "MX51VA2L0088", "VXGUBCP B1CO", "2013-05-17", "2026-03-02", "3,111", ""),
        ("VXGUBCP", "B0CO", "PM con Fee", "MX51VA2L0054", "VXGUBCP B0CO", "2008-07-04", "2026-03-02", "3,760", ""),

        ("VXUDIMP", "A", "Serie A", "MX51VA2S0008", "VXUDIMP A", "2004-04-22", "2026-03-02", "4,051", ""),
        ("VXUDIMP", "B1FI", "Persona Fisica", "MX51VA2S0073", "VXUDIMP B1FI", "2004-04-22", "2026-03-02", "5,496", ""),
        ("VXUDIMP", "B0FI", "PF con Fee", "MX51VA2S0040", "VXUDIMP B0FI", "2005-09-06", "2026-03-02", "5,142", ""),
        ("VXUDIMP", "B1NC", "PPR", "MX51VA2S0081", "VXUDIMP B1NC", "2008-08-11", "2026-03-02", "4,077", ""),
        ("VXUDIMP", "B1CO", "Persona Moral", "MX51VA2S0099", "VXUDIMP B1CO", "2009-08-13", "2026-03-02", "4,159", ""),
        ("VXUDIMP", "B0CO", "PM con Fee", "MX51VA2S0065", "VXUDIMP B0CO", "2008-07-04", "2026-03-02", "4,366", ""),

        ("VXDEUDA", "A", "Serie A", "MXP800521009", "VXDEUDA A", "2000-11-01", "2026-03-02", "4,895", ""),
        ("VXDEUDA", "B1FI", "Persona Fisica", "MX51VA2M00A3", "VXDEUDA B1FI", "2008-01-04", "2026-03-02", "4,557", ""),
        ("VXDEUDA", "B0FI", "PF con Fee", "MX51VA2M0061", "VXDEUDA B0FI", "2009-09-11", "2026-03-02", "4,138", ""),
        ("VXDEUDA", "B1CF", "PPR", "MX51VA2M0095", "VXDEUDA B1CF", "2010-04-23", "2026-03-02", "2,950", ""),
        ("VXDEUDA", "B1CO", "Persona Moral", "MX51VA2M00E5", "VXDEUDA B1CO", "2008-02-28", "2026-03-02", "4,519", ""),
        ("VXDEUDA", "B0CO", "PM con Fee", "MX51VA2M00D7", "VXDEUDA B0CO", "2010-12-29", "2026-03-02", "2,886", ""),

        ("VXGUBLP", "A", "Serie A", "MX51VA2R0009", "VXGUBLP A", "2004-04-22", "2026-03-02", "4,051", ""),
        ("VXGUBLP", "B1FI", "Persona Fisica", "MX51VA2R0066", "VXGUBLP B1FI", "2004-04-22", "2026-03-02", "5,496", ""),
        ("VXGUBLP", "B0FI", "PF con Fee", "MX51VA2R0041", "VXGUBLP B0FI", "2005-09-06", "2026-03-02", "5,141", ""),
        ("VXGUBLP", "B1CF", "PPR", "MX51VA2R00D6", "VXGUBLP B1CF", "2023-07-07", "2026-03-02", "664", ""),
        ("VXGUBLP", "B1CO", "Persona Moral", "MX51VA2R0082", "VXGUBLP B1CO", "2011-11-11", "2026-03-02", "3,592", ""),
        ("VXGUBLP", "B0CO", "PM con Fee", "MX51VA2R00F1", "VXGUBLP B0CO", "2008-10-15", "2026-03-02", "3,220", ""),
    ],
    "Deuda USD": [
        ("VXTBILL", "A", "Serie A", "MX51VA1F0004", "VXTBILL A", "2016-02-17", "2026-03-02", "2,508", ""),
        ("VXTBILL", "B0FI", "PF / PF con Fee", "MX51VA1F0012", "VXTBILL B0FI", "2019-03-04", "2026-03-02", "1,759", ""),
        ("VXTBILL", "B0CF", "PPR", "MX51VA1F0087", "VXTBILL B1CF", "2024-09-18", "2026-03-02", "362", "ISIN es de B1CF"),
        ("VXTBILL", "B0CO", "PM / PM con Fee", "MX51VA1F0020", "VXTBILL B0CO", "2019-03-06", "2026-03-02", "1,757", ""),

        ("VXCOBER", "A", "Serie A", "MXP800621007", "VXCOBER A", "2000-11-01", "2026-03-02", "4,897", ""),
        ("VXCOBER", "B1FI", "Persona Fisica", "MX51VA2N0060", "VXCOBER B1FI", "2000-11-01", "2026-03-02", "6,342", ""),
        ("VXCOBER", "B0FI", "PF con Fee", "MX51VA2N0037", "VXCOBER B0FI", "2005-09-06", "2026-03-02", "5,142", ""),
        ("VXCOBER", "B1CF", "PPR", "MX51VA2N00D5", "VXCOBER B1CF", "2019-04-17", "2026-03-02", "1,728", ""),
        ("VXCOBER", "B1CO", "Persona Moral", "MX51VA2N0086", "VXCOBER B1CO", "2011-09-26", "2026-03-02", "3,504", ""),
        ("VXCOBER", "B0CO", "PM con Fee", "", "", "", "", "", ""),
    ],
    "ETFs / Deuda Mixta": [
        ("VLMXETF", "A", "Serie A", "MX52VL060004", "VLMXETF A", "2000-11-01", "2026-03-02", "4,898", ""),
        ("VLMXETF", "B1FI", "Persona Fisica", "MX52VL060079", "VLMXETF B1FI", "2000-11-01", "2026-03-02", "6,308", ""),
        ("VLMXETF", "B0FI", "PF con Fee", "MX52VL060038", "VLMXETF B0FI", "2005-09-06", "2026-03-02", "5,038", ""),
        ("VLMXETF", "B1CF", "PPR", "", "", "", "", "", ""),
        ("VLMXETF", "B1CO", "Persona Moral", "MX52VL060061", "VLMXETF B1CO", "2011-08-03", "2026-03-02", "3,577", ""),
        ("VLMXETF", "B0CO", "PM con Fee", "", "", "", "", "", ""),

        ("VLMXDME", "A", "Serie A", "MX52VL0D0002", "VLMXDME A", "2020-04-14", "2026-03-02", "1,482", ""),
        ("VLMXDME", "B1FI", "Persona Fisica", "MX52VL0D00B0", "VLMXDME B1FI", "2022-08-18", "2026-03-02", "887", ""),
        ("VLMXDME", "B0FI", "PF con Fee", "MX52VL0D0036", "VLMXDME B0FI", "2022-08-16", "2026-03-02", "889", ""),
        ("VLMXDME", "B1CF", "PPR", "MX52VL0D0051", "VLMXDME B1CF", "2023-07-07", "2026-03-02", "665", ""),
        ("VLMXDME", "B1CO", "Persona Moral", "", "", "", "", "", ""),
        ("VLMXDME", "B0CO", "PM con Fee", "MX52VL0D0028", "VLMXDME B0CO", "2023-03-03", "2026-03-02", "751", ""),
    ],
    "Renta Variable": [
        ("VALMXA", "A", "Serie A", "MX52VA2W0000", "VALMXA A", "2005-10-14", "2026-03-02", "3,670", ""),
        ("VALMXA", "B1", "PF / PPR / PM", "MX52VA2W0026", "VALMXA B1", "2005-10-14", "2026-03-02", "5,116", ""),
        ("VALMXA", "B0", "PF Fee / PM Fee", "MX52VA2W0018", "VALMXA B0", "2005-10-14", "2026-03-02", "5,116", ""),

        ("VALMX20", "A", "Serie A", "MXP800541007", "VALMX20 A", "2000-11-01", "2026-03-02", "4,899", ""),
        ("VALMX20", "B1", "PF / PPR / PM", "MX52VA2O0000", "VALMX20 B1", "2000-11-01", "2026-03-02", "6,344", ""),
        ("VALMX20", "B0", "PF Fee / PM Fee", "MX52VA2O0026", "VALMX20 B0", "2005-09-28", "2026-03-02", "5,129", ""),

        ("VALMX28", "A", "Serie A", "MX52VA130008", "VALMX28 A", "2016-01-18", "2026-03-02", "2,544", ""),
        ("VALMX28", "B1FI", "Persona Fisica", "MX52VA130016", "VALMX28 B1FI", "2011-02-01", "2026-03-02", "3,790", ""),
        ("VALMX28", "B0FI", "PF con Fee", "MX52VA130065", "VALMX28 B0FI", "2013-02-15", "2026-03-02", "3,276", ""),
        ("VALMX28", "B1NC", "PPR", "MX52VA1300C8", "VALMX28 B1NC", "2024-07-23", "2026-03-02", "403", ""),
        ("VALMX28", "B1CO", "Persona Moral", "MX52VA1300B0", "VALMX28 B1CO", "2024-07-23", "2026-01-28", "381", ""),
        ("VALMX28", "B0CO", "PM con Fee", "MX52VA130032", "VALMX28 B0CO", "2015-07-10", "2026-03-02", "2,649", ""),

        ("VALMXVL", "A", "Serie A", "MX52VA140007", "VALMXVL A", "2000-11-01", "2026-03-02", "4,712", ""),
        ("VALMXVL", "B1", "PF / PPR / PM", "MX52VA140023", "VALMXVL B1", "2000-11-01", "2026-03-02", "5,494", ""),
        ("VALMXVL", "B0", "PF Fee / PM Fee", "MX52VA140015", "VALMXVL B0", "2005-09-28", "2026-03-02", "4,267", ""),

        ("VALMXES", "A", "Serie A", "MX52VA190002", "VALMXES A", "2004-04-22", "2026-03-02", "4,053", ""),
        ("VALMXES", "B1", "PF / PPR / PM", "MX52VA190028", "VALMXES B1", "2015-10-06", "2026-03-02", "2,614", ""),
        ("VALMXES", "B0", "PF Fee / PM Fee", "MX52VA190010", "VALMXES B0", "2015-10-06", "2026-03-02", "2,614", ""),

        ("VLMXTEC", "A", "Serie A", "MX52VL080002", "VLMXTEC A", "2019-12-03", "2026-03-02", "1,570", ""),
        ("VLMXTEC", "B1FI", "Persona Fisica", "MX52VL080069", "VLMXTEC B1FI", "2020-01-08", "2026-03-02", "1,547", ""),
        ("VLMXTEC", "B0FI", "PF con Fee", "MX52VL080036", "VLMXTEC B0FI", "2020-01-02", "2026-03-02", "1,551", ""),
        ("VLMXTEC", "B1CF", "PPR", "MX52VL080051", "VLMXTEC B1CF", "2020-04-17", "2026-03-02", "1,479", ""),
        ("VLMXTEC", "B1CO", "Persona Moral", "MX52VL080077", "VLMXTEC B1CO", "2020-01-20", "2026-03-02", "1,539", ""),
        ("VLMXTEC", "B0CO", "PM con Fee", "MX52VL080028", "VLMXTEC B0CO", "2019-12-17", "2026-03-02", "1,561", ""),

        ("VLMXESG", "A", "Serie A", "MX52VL0B0004", "VLMXESG A", "2020-12-23", "2026-03-02", "1,305", ""),
        ("VLMXESG", "B1FI", "Persona Fisica", "MX52VL0B0046", "VLMXESG B1FI", "2020-12-24", "2026-03-02", "1,304", ""),
        ("VLMXESG", "B0FI", "PF con Fee", "MX52VL0B0012", "VLMXESG B0FI", "2020-12-24", "2026-03-02", "1,304", ""),
        ("VLMXESG", "B1CF", "PPR", "MX52VL0B0079", "VLMXESG B1CF", "2021-02-12", "2026-03-02", "1,271", ""),
        ("VLMXESG", "B1CO", "Persona Moral", "MX52VL0B0053", "VLMXESG B1CO", "2021-01-20", "2026-03-02", "1,287", ""),
        ("VLMXESG", "B0CO", "PM con Fee", "MX52VL0B00D0", "VLMXESG B0CO", "2021-01-08", "2026-03-02", "1,295", ""),

        ("VALMXHC", "A", "Serie A", "MX52VA1L0004", "VALMXHC A", "2021-09-02", "2026-03-02", "1,130", ""),
        ("VALMXHC", "B1FI", "Persona Fisica", "MX52VA1L00D0", "VALMXHC B1FI", "2021-09-08", "2026-03-02", "1,126", ""),
        ("VALMXHC", "B0FI", "PF con Fee", "MX52VA1L0012", "VALMXHC B0FI", "2021-09-06", "2026-03-02", "1,128", ""),
        ("VALMXHC", "B1CF", "PPR", "MX52VA1L0087", "VALMXHC B1CF", "2021-12-28", "2026-03-02", "1,050", ""),
        ("VALMXHC", "B1CO", "Persona Moral", "MX52VA1L0061", "VALMXHC B1CO", "2021-09-13", "2026-03-02", "1,123", ""),
        ("VALMXHC", "B0CO", "PM con Fee", "MX52VA1L0020", "VALMXHC B0CO", "2021-09-02", "2026-03-02", "1,130", ""),

        ("VXINFRA", "A", "Serie A", "MX52VL0E0001", "VXINFRA A", "2023-04-17", "2026-03-02", "722", ""),
        ("VXINFRA", "B1FI", "PF / PPR", "MX52VL0E0050", "VXINFRA B1FI", "2023-06-07", "2026-03-02", "687", ""),
        ("VXINFRA", "B0FI", "PF con Fee", "MX52VL0E0027", "VXINFRA B0FI", "2024-09-04", "2026-03-02", "371", ""),
        ("VXINFRA", "B0CO", "PM con Fee", "MX52VL0E0019", "VXINFRA B0CO", "2024-08-30", "2026-03-02", "375", ""),
    ],
    "Fondos de Ciclo": [
        ("VLMXJUB", "A", "Serie A", "MX52VL070003", "VLMXJUB A", "2016-01-18", "2026-03-02", "2,544", ""),
        ("VLMXJUB", "B1FI", "Persona Fisica", "MX52VL070078", "VLMXJUB B1FI", "2020-09-30", "2026-03-02", "1,363", ""),
        ("VLMXJUB", "B0FI", "PF con Fee", "", "", "", "", "", ""),
        ("VLMXJUB", "B1CF", "PPR", "MX52VL070052", "VLMXJUB B1CF", "2020-10-07", "2026-03-02", "1,358", ""),

        ("VLMXP24", "A", "Serie A", "MX52VL010009", "VLMXP24 A", "2016-01-18", "2026-03-02", "2,544", ""),
        ("VLMXP24", "B1FI", "Persona Fisica", "MX52VL010041", "VLMXP24 B1FI", "2012-07-26", "2026-03-02", "2,295", ""),
        ("VLMXP24", "B0FI", "PF con Fee", "", "", "", "", "", ""),
        ("VLMXP24", "B1NC", "PPR", "MX52VL010058", "VLMXP24 B1NC", "2013-12-10", "2026-03-02", "3,070", ""),

        ("VLMXP31", "A", "Serie A", "MX52VL030007", "VLMXP31 A", "2016-01-18", "2026-03-02", "2,544", ""),
        ("VLMXP31", "B1FI", "Persona Fisica", "MX52VL030049", "VLMXP31 B1FI", "2016-09-07", "2026-03-02", "2,146", ""),
        ("VLMXP31", "B0FI", "PF con Fee", "MX52VL030015", "VLMXP31 B0FI", "2020-10-14", "2026-03-02", "1,353", ""),
        ("VLMXP31", "B1CF", "PPR", "MX52VL030049", "VLMXP31 B1FI", "2016-09-07", "2026-03-02", "2,146", "ISIN duplicado (=B1FI)"),

        ("VLMXP38", "A", "Serie A", "MX52VL000000", "VLMXP38 A", "2016-01-18", "2026-03-02", "2,544", ""),
        ("VLMXP38", "B1FI", "Persona Fisica", "MX52VL000042", "VLMXP38 B1FI", "2011-06-09", "2016-04-05", "1,079", "Serie cerrada"),
        ("VLMXP38", "B0FI", "PF con Fee", "MX52VL000018", "VLMXP38 B0FI", "2012-09-19", "2026-03-02", "2,161", ""),
        ("VLMXP38", "B1CF", "PPR", "MX52VL0000B4", "VLMXP38 B1CF", "2017-09-18", "2026-03-02", "1,642", ""),

        ("VLMXP45", "A", "Serie A", "MX52VL040014", "VLMXP45 A", "2016-01-18", "2026-03-02", "2,544", ""),
        ("VLMXP45", "B1FI", "Persona Fisica", "MX52VL040071", "VLMXP45 B1FI", "2021-05-20", "2026-03-02", "1,205", ""),
        ("VLMXP45", "B0FI", "PF con Fee", "MX52VL040022", "VLMXP45 B0FI", "2021-12-15", "2026-03-02", "1,059", ""),
        ("VLMXP45", "B1CF", "PPR", "MX52VL040097", "VLMXP45 B1CF", "2017-09-18", "2026-03-02", "2,117", ""),

        ("VLMXP52", "A", "Serie A", "MX52VL050005", "VLMXP52 A", "2018-11-12", "2026-03-02", "1,834", ""),
        ("VLMXP52", "B1FI", "Persona Fisica", "MX52VL050047", "VLMXP52 B1FI", "2021-07-16", "2026-03-02", "1,164", ""),
        ("VLMXP52", "B0FI", "PF con Fee", "MX52VL050013", "VLMXP52 B0FI", "2022-01-21", "2026-03-02", "1,032", ""),
        ("VLMXP52", "B1NC", "PPR", "MX52VL050096", "VLMXP52 B1NC", "2021-01-18", "2026-03-02", "1,289", ""),

        ("VLMXP59", "A", "Serie A", "MX52VL0C0003", "VLMXP59 A", "2021-09-02", "2026-03-02", "1,130", ""),
        ("VLMXP59", "B1FI", "Persona Fisica", "MX52VL0C0086", "VLMXP59 B1FI", "2024-07-26", "2026-03-02", "400", ""),
        ("VLMXP59", "B0FI", "PF con Fee", "", "", "", "", "", ""),
        ("VLMXP59", "B1NC", "PPR", "MX52VL0C0052", "VLMXP59 B1NC", "2021-12-30", "2026-03-02", "1,048", ""),
    ],
}


pdf = SeriesPDF(orientation="L", format="letter")
pdf.alias_nb_pages()
pdf.set_auto_page_break(auto=True, margin=15)

for section, rows in data.items():
    pdf.add_page()
    pdf.section_title(section)
    pdf.table_header()
    for i, r in enumerate(rows):
        row_dict = {
            "fondo": r[0], "serie": r[1], "tipo": r[2],
            "isin": r[3], "api_name": r[4],
            "desde": r[5], "hasta": r[6], "count": r[7], "notas": r[8],
        }
        # Check page space
        if pdf.get_y() > 185:
            pdf.add_page()
            pdf.section_title(f"{section} (cont.)")
            pdf.table_header()
        pdf.table_row(row_dict, shade=(i % 2 == 1))

# ── Backtesting Validation: Gaps & NAV Resets ──
gaps_cols = [
    ("Fondo", 20), ("Serie", 12), ("ISIN", 32),
    ("Fecha Antes", 22), ("Valor Antes", 18),
    ("Fecha Despues", 22), ("Valor Despues", 18),
    ("Meses Sin Dato", 20), ("Retorno Acum.", 18), ("Anualizado", 14),
]
gaps_data = [
    # (fondo, serie, isin, fecha_antes, valor_antes, fecha_despues, valor_despues, meses_gap, retorno, anualizado)
    ("VALMX20", "A", "MXP800541007", "2011-10-01", "103.3", "2016-01-01", "137.9", "~51", "+33.4%", "~7%/a"),
    ("VALMXA", "A", "MX52VA2W0000", "2011-10-01", "101.7", "2016-01-01", "140.8", "~51", "+38.5%", "~8%/a"),
    ("VALMXVL", "B2", "MX52VA140031", "2010-07-01", "94.7", "2014-01-01", "56.2", "~42", "-40.6%", "~-13%/a"),
    ("VXCOBER", "A", "MXP800621007", "2011-10-01", "96.6", "2016-01-01", "140.3", "~51", "+45.2%", "~9%/a"),
    ("VXDEUDA", "A", "MXP800521009", "2013-12-01", "101.6", "2016-01-01", "132.6", "~24", "+30.5%", "~14%/a"),
    ("VXDEUDA", "B0CO", "MX51VA2M00D7", "2016-04-01", "118.2", "2019-12-01", "156.7", "~44", "+32.5%", "~8%/a"),
    ("VXDEUDA", "B1CF", "MX51VA2M0095", "2016-04-01", "119.0", "2019-07-01", "159.6", "~39", "+34.2%", "~9%/a"),
    ("VXGUBCP", "B0CF", "MX51VA2L00B3", "2016-04-01", "104.2", "2019-08-01", "148.9", "~40", "+42.8%", "~11%/a"),
    ("VXGUBLP", "A", "MX51VA2R0009", "2011-10-01", "103.2", "2016-01-01", "150.5", "~51", "+45.9%", "~9%/a"),
    ("VXGUBLP", "B2CO", "MX51VA2R00B0", "2021-03-01", "165.0", "2025-10-01", "241.9", "~55", "+46.7%", "~9%/a"),
    ("VLMXP24", "B1FI", "MX52VL010041", "2019-01-01", "115.6", "2023-07-01", "161.8", "~54", "+40.0%", "~8%/a"),
    ("VLMXP38", "B0FI", "MX52VL000018", "2016-04-01", "106.4", "2021-02-01", "172.5", "~58", "+62.1%", "~10%/a"),
]

pdf.add_page()
pdf.section_title("Analisis de Gaps en Datos (Backtesting Base-100)")
pdf.set_font("Helvetica", "", 7)
pdf.cell(0, 4, "Periodos donde la serie mensual no tiene precios, causando un salto visible en la grafica.", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 4, "Los retornos acumulados son correctos (representan el rendimiento real del periodo sin datos).", new_x="LMARGIN", new_y="NEXT")
pdf.ln(2)
pdf.table_header(gaps_cols)
for i, g in enumerate(gaps_data):
    pdf.set_font("Helvetica", "", 6.5)
    shade = (i % 2 == 1)
    if shade:
        pdf.set_fill_color(245, 245, 250)
    else:
        pdf.set_fill_color(255, 255, 255)
    for j, (_, w) in enumerate(gaps_cols):
        pdf.cell(w, 4.5, g[j], border=1, fill=shade, align="C")
    pdf.ln()

pdf.ln(4)

# NAV Reset section
reset_cols = [
    ("Fondo", 20), ("Serie", 12), ("ISIN", 32),
    ("Fecha Reset", 22), ("NAV Antes", 22),
    ("NAV Despues", 22), ("Factor", 18), ("Accion Tomada", 48),
]
reset_data = [
    ("VXTBILL", "A", "MX51VA1F0004", "2018-04-17", "0.0151", "15.0789", "998.6x", "Ajuste automatico de precios previos"),
]

pdf.section_title("Resets de NAV Detectados")
pdf.set_font("Helvetica", "", 7)
pdf.cell(0, 4, "Cambios de precio >2x en un dia indican reestructuracion de serie (no rendimiento real).", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 4, "El sistema ajusta automaticamente los precios anteriores al reset para mantener continuidad.", new_x="LMARGIN", new_y="NEXT")
pdf.ln(2)
pdf.table_header(reset_cols)
for i, r in enumerate(reset_data):
    pdf.set_font("Helvetica", "", 6.5)
    pdf.set_fill_color(255, 255, 255)
    for j, (_, w) in enumerate(reset_cols):
        pdf.cell(w, 4.5, r[j], border=1, align="C")
    pdf.ln()

pdf.ln(4)

# Series sin datos
nodata_cols = [
    ("Fondo", 24), ("Serie", 14), ("ISIN", 36),
    ("Ultimo Precio", 26), ("Razon", 50), ("Accion Tomada", 46),
]
nodata_data = [
    ("VLMXP38", "B1FI", "MX52VL000042", "2016-04-05", "Serie cerrada (sin precios en >90 dias)", "Excluida del backtesting"),
]

pdf.section_title("Series Cerradas / Sin Datos")
pdf.set_font("Helvetica", "", 7)
pdf.cell(0, 4, "Series cuyo ultimo precio es anterior a 90 dias. No se incluyen en el backtesting.", new_x="LMARGIN", new_y="NEXT")
pdf.ln(2)
pdf.table_header(nodata_cols)
for i, r in enumerate(nodata_data):
    pdf.set_font("Helvetica", "", 6.5)
    pdf.set_fill_color(255, 230, 230)
    for j, (_, w) in enumerate(nodata_cols):
        pdf.cell(w, 4.5, r[j], border=1, fill=True, align="C")
    pdf.ln()

# Summary page
pdf.add_page()
pdf.section_title("Resumen")
pdf.ln(3)
pdf.set_font("Helvetica", "", 9)
lines = [
    "103 ISINs verificados contra la API NAV de Morningstar (UnadjustedNAV por ISIN).",
    "",
    "Validacion de ISINs:",
    "  - 93 ISINs correctos y con datos vigentes al 2026-03-02",
    "  - 7 series sin ISIN en el sistema (espacios vacios en la tabla):",
    "      VXCOBER B0CO, VLMXETF B1CF, VLMXETF B0CO, VLMXDME B1CO,",
    "      VLMXJUB B0FI, VLMXP24 B0FI, VLMXP59 B0FI",
    "  - 1 ISIN corregido: VXTBILL B0CF -> B1CF (MX51VA1F0087 regresa B1CF de la API)",
    "  - 1 ISIN duplicado removido: VLMXP31 B1CF (misma ISIN que B1FI: MX52VL030049)",
    "  - 1 serie cerrada: VLMXP38 B1FI - datos solo hasta 2016-04-05",
    "",
    "Validacion de Backtesting (195 series, base-100):",
    "  - 182 series OK: serie continua sin anomalias",
    "  - 12 series con gaps: periodos sin datos que causan saltos visibles (ver tabla)",
    "      Los retornos son correctos, solo la cobertura temporal es discontinua",
    "  - 1 serie sin datos: VLMXP38 B1FI (cerrada, excluida automaticamente)",
    "  - 1 reset NAV detectado: VXTBILL A (998.6x en 2018-04-17, ajustado automaticamente)",
    "",
    "SERIE_MAP en el codigo coincide exactamente con el PDF de series proporcionado.",
    "",
    "Verificacion cruzada con API del Universo Morningstar:",
    "  56 entradas en el universo, todas coinciden con ISIN_MAP.",
    "",
    "Acciones correctivas aplicadas:",
    "  - Validacion de fundName: verifica que el ISIN regrese el fondo+serie esperado",
    "  - Deteccion de resets NAV: ajusta precios antes de cambios >2x diarios",
    "  - Freshness check: excluye series sin precio en los ultimos 90 dias",
]
for line in lines:
    pdf.cell(0, 5, line, new_x="LMARGIN", new_y="NEXT")

import os as _os
out = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "Series_Validacion_Morningstar.pdf")
pdf.output(out)
print(f"PDF generado: {out}")

def main(
    gross_monthly: float = 0,
    pay_months: int = 14,
    pendlerpauschale: float = 0,
    revenue: float = 0,
    expenses: float = 0,
    use_pauschal: bool = False,
    age: int = 32,
    children: int = 0,
    state: str = "wien",
    include_context: bool = True
) -> dict:
    """Austrian Tax Calculator API - Reference Year 2026

    Calculate net income, taxes, and social insurance for employment,
    freelance, or hybrid scenarios in Austria.
    """

    # Config Austria 2026
    config = {
        "asvg": {
            "minorThreshold": 551.1, "monthlyHBG": 6930, "annualHBG": 83160, "bonusHBG": 13860,
            "empRates": {"kv": 0.0387, "pv": 0.1025, "alv": 0.0295, "ak": 0.005, "wbf": 0.005},
            "erRates": {"kv": 0.0378, "pv": 0.1255, "alv": 0.0295, "uv": 0.011, "misc": 0.006, "db": 0.039, "kt": 0.03, "mvk": 0.0153}
        },
        "alv": {"tiers": [(2225, 0), (2427, 0.01), (2630, 0.02)], "std": 0.0295, "exempt": 63},
        "gsvg": {"annualHBG": 97020, "kv": 0.068, "pv": 0.185, "uv": 12.95, "min": 160, "pausch": 0.15, "pauschMax": 63000, "profitAllow": 0.15, "profitCap": 33000},
        "dz": {"wien": 0.0036, "burgenland": 0.004, "niederoesterreich": 0.004, "oberoesterreich": 0.0036, "steiermark": 0.0039, "kaernten": 0.0035, "salzburg": 0.0045, "tirol": 0.0046, "vorarlberg": 0.004},
        "tax": {"brackets": [(13539, 0), (21992, 0.2), (36458, 0.3), (70365, 0.4), (104859, 0.48), (1000000, 0.5), (None, 0.55)]},
        "credits": {"family": 2000, "vab": 496, "vabPendler": 853},
        "bonus": {"rate": 0.06, "free": 620, "max": 25000}
    }

    # ALV rate
    alv_rate = 0 if age >= config["alv"]["exempt"] else next((r for m, r in config["alv"]["tiers"] if gross_monthly <= m), config["alv"]["std"])

    # Employment
    reg_months = min(12, pay_months)
    bonus_months = max(pay_months - 12, 0)
    monthly_base = min(gross_monthly, config["asvg"]["monthlyHBG"])

    emp_rates = config["asvg"]["empRates"]
    reg_sv = monthly_base * (emp_rates["kv"] + emp_rates["pv"] + alv_rate + emp_rates["ak"] + emp_rates["wbf"]) * reg_months

    bonus_gross = gross_monthly * bonus_months
    bonus_base = min(bonus_gross, config["asvg"]["bonusHBG"])
    bonus_sv = bonus_base * (emp_rates["kv"] + emp_rates["pv"] + alv_rate)
    employment_sv = reg_sv + bonus_sv

    reg_gross = gross_monthly * reg_months
    annual_gross = reg_gross + bonus_gross
    taxable_emp = max(reg_gross - reg_sv - pendlerpauschale, 0)

    # Bonus tax (6%)
    bonus_taxable = max(bonus_gross - bonus_sv, 0)
    bonus_tax_base = max(bonus_taxable - config["bonus"]["free"], 0)
    bonus_tax = min(bonus_tax_base, config["bonus"]["max"]) * config["bonus"]["rate"]

    # Employer cost
    dz = config["dz"].get(state.lower(), 0.0036)
    er_rates = config["asvg"]["erRates"]
    er_rate = sum(er_rates.values()) + dz
    employer_cost = annual_gross + (monthly_base * er_rate * pay_months)

    # Freelance
    pausch_exp = revenue * config["gsvg"]["pausch"]
    deductible = min(pausch_exp, config["gsvg"]["pauschMax"]) if use_pauschal else expenses
    base_profit = max(revenue - deductible, 0)

    # SV gap for hybrid
    sv_gap = config["gsvg"]["annualHBG"]
    if gross_monthly >= config["asvg"]["minorThreshold"] and annual_gross > 0:
        sv_gap = max(config["gsvg"]["annualHBG"] - (monthly_base * reg_months + bonus_base), 0)

    sv_base = min(base_profit, sv_gap)
    freelance_sv = sv_base * (config["gsvg"]["kv"] + config["gsvg"]["pv"]) + config["gsvg"]["uv"] * 12

    # Profit allowance
    profit_allow = min(base_profit, config["gsvg"]["profitCap"]) * config["gsvg"]["profitAllow"]
    taxable_free = max(base_profit - freelance_sv - profit_allow, 0)

    # Progressive tax
    def calc_tax(income, is_emp, has_pendler):
        remaining = max(income, 0)
        last = 0
        tax = 0
        for limit, rate in config["tax"]["brackets"]:
            lim = limit if limit else remaining + last
            band = min(remaining, max(lim - last, 0))
            if band <= 0:
                last = lim
                continue
            tax += band * rate
            remaining -= band
            last = lim
            if remaining <= 0:
                break
        if is_emp and income > 0:
            vab = config["credits"]["vabPendler"] if has_pendler else config["credits"]["vab"]
            tax = max(tax - vab, 0)
        return tax

    has_pendler = pendlerpauschale > 0
    has_emp = taxable_emp > 0
    emp_tax_reg = calc_tax(taxable_emp, True, has_pendler)
    total_prog_tax = calc_tax(taxable_emp + taxable_free, has_emp, has_emp and has_pendler)
    freelance_tax = total_prog_tax - emp_tax_reg

    # Family bonus
    max_family = max(children, 0) * config["credits"]["family"]
    remaining_bonus = max_family
    emp_tax_final = emp_tax_reg + bonus_tax
    if remaining_bonus > 0:
        used = min(remaining_bonus, emp_tax_final)
        emp_tax_final -= used
        remaining_bonus -= used
    free_tax_final = freelance_tax
    if remaining_bonus > 0:
        used = min(remaining_bonus, free_tax_final)
        free_tax_final -= used
        remaining_bonus -= used

    # Totals
    total_tax = emp_tax_final + free_tax_final
    total_sv = employment_sv + freelance_sv
    total_revenue = annual_gross + revenue
    emp_net = taxable_emp + bonus_taxable - emp_tax_final
    free_net = revenue - freelance_sv - free_tax_final
    total_net = emp_net + free_net

    # Pills
    pills = []
    if 0 < gross_monthly < config["asvg"]["minorThreshold"]:
        pills.append({"id": "minor", "label": "Keine Vollversicherung", "severity": "warning", "url": "https://www.usp.gv.at/themen/mitarbeiter-und-gesundheit/einstellung-mitarbeiter-und-arten-der-beschaeftigung/geringfuegig-beschaeftigte.html"})
    if bonus_months > 0:
        pills.append({"id": "bonus", "label": "13./14. Gehalt", "severity": "info", "url": "https://www.wko.at/lohnverrechnung/sonstige-bezuege-steuerliche-behandlung"})
    if use_pauschal:
        pills.append({"id": "pauschal", "label": "Basispauschalierung aktiv", "severity": "info", "url": "https://www.wko.at/steuern/basispauschalierung"})
    if 0 < revenue <= 55000:
        pills.append({"id": "kleinunternehmer", "label": "Kleinunternehmer", "severity": "info", "url": "https://www.usp.gv.at/themen/steuern-finanzen/umsatzsteuer-ueberblick/weitere-informationen-zur-umsatzsteuer/weitere-steuertatbestaende-und-befreiungen/kleinunternehmen.html"})
    if has_pendler:
        pills.append({"id": "pendler", "label": "Pendlerpauschale aktiv", "severity": "info", "url": "https://www.bmf.gv.at/themen/steuern/arbeitnehmerveranlagung/pendlerfoerderung-das-pendlerpauschale/allgemeines-zum-pendlerpauschale.html"})
    if children > 0:
        pills.append({"id": "familybonus", "label": f"Familienbonus: €{max_family - remaining_bonus}", "severity": "info", "url": "https://www.bmf.gv.at/themen/familie/familienbonus-plus.html"})

    result = {
        "success": True,
        "input": {"gross_monthly": gross_monthly, "pay_months": pay_months, "pendlerpauschale": pendlerpauschale, "revenue": revenue, "expenses": expenses, "use_pauschal": use_pauschal, "age": age, "children": children, "state": state},
        "results": {
            "summary": {"annual_net_income": round(total_net, 2), "total_tax": round(total_tax, 2), "total_social_insurance": round(total_sv, 2), "total_revenue": round(total_revenue, 2), "employer_cost": round(employer_cost, 2)},
            "rates": {"effective_tax_rate": round(total_tax / total_revenue * 100, 2) if total_revenue > 0 else 0, "effective_sv_rate": round(total_sv / total_revenue * 100, 2) if total_revenue > 0 else 0, "total_burden_rate": round((total_tax + total_sv) / total_revenue * 100, 2) if total_revenue > 0 else 0},
            "employment": {"annual_gross": annual_gross, "net_annual": round(emp_net, 2), "sv": round(employment_sv, 2), "tax": round(emp_tax_final, 2), "employer_cost": round(employer_cost, 2)} if annual_gross > 0 else None,
            "freelance": {"revenue": revenue, "net_after_tax": round(free_net, 2), "sv": round(freelance_sv, 2), "tax": round(free_tax_final, 2), "expenses_deducted": deductible} if revenue > 0 else None
        },
        "pills": pills
    }

    if include_context:
        result["context"] = {
            "description": "Austrian Tax Calculator 2026. Calculates net income, taxes, and social insurance.",
            "glossary": {"Brutto": "Gross income", "Netto": "Net income", "SV": "Social insurance", "13./14. Gehalt": "Bonus months taxed at 6%", "Familienbonus": "€2000/child tax credit", "Kleinunternehmer": "VAT exempt <€55k", "Basispauschalierung": "15% flat expense deduction"}
        }

    return result

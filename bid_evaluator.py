from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any

from field_config import NOT_AVAILABLE
from tender_extractor import PageText

CENTRAL_UTILITY_KEYWORDS = ("ntpc", "nhpc", "seci", "powergrid", "india", "national thermal", "renewable energy")
PUBLIC_ENTITY_KEYWORDS = (
    "government",
    "jal nigam",
    "nigam",
    "authority",
    "board",
    "department",
    "corporation",
    "municipal",
    "parishad",
    "council",
    "mission",
)


@dataclass(frozen=True, slots=True)
class EvaluationCriterion:
    row_number: int
    point: str
    weight: float
    allocation: float
    selected_band: str
    weighted_score: float
    rationale: str
    source: str
    inferred: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["allocation_percent"] = round(self.allocation * 100, 2)
        payload["weight_percent"] = round(self.weight * 100, 2)
        payload["weighted_percent"] = round(self.weighted_score * 100, 2)
        return payload


class BidEvaluationAnalyzer:
    def analyze(self, extraction_bundle: dict[str, object], pages: list[PageText]) -> dict[str, object]:
        row_map = self._build_row_map(extraction_bundle.get("rows", []))
        full_text = "\n".join(" ".join(page.lines) for page in pages).lower()

        criteria = [
            self._evaluate_payment_terms(row_map),
            self._evaluate_lumpsum_quantities(row_map, full_text),
            self._evaluate_funding(row_map),
            self._evaluate_price_variation(row_map),
            self._evaluate_completion_period(row_map),
            self._evaluate_liquidated_damages(full_text),
            self._evaluate_customer(row_map),
            self._evaluate_contractual_risk(full_text),
            self._evaluate_performance_security(full_text),
            self._evaluate_technical_flexibility(full_text),
            self._evaluate_site_readiness(full_text),
            self._evaluate_payment_track_record(row_map),
            self._evaluate_joint_venture(full_text),
            self._evaluate_emd(row_map),
            self._evaluate_site_conditions(full_text),
        ]

        total_fraction = round(sum(item.weighted_score for item in criteria), 6)
        total_percentage = round(total_fraction * 100, 2)
        category, decision, category_row = self._categorize(total_fraction)
        work_title = self._clean_work_title(self._value_or_default(row_map, 7))

        return {
            "customer": self._value_or_default(row_map, 5),
            "tender_ref": self._value_or_default(row_map, 4),
            "work_title": work_title,
            "bid_submission_due_date": self._value_or_default(row_map, 20),
            "report_date": extraction_bundle.get("report_date", NOT_AVAILABLE),
            "total_fraction": total_fraction,
            "total_percentage": total_percentage,
            "category": category,
            "decision": decision,
            "category_row": category_row,
            "criteria": [item.to_dict() for item in criteria],
        }

    def _evaluate_payment_terms(self, row_map: dict[int, dict[str, Any]]) -> EvaluationCriterion:
        supply_text = " ".join(
            self._value_or_default(row_map, row_number)
            for row_number in (34, 35, 39, 40, 41)
        )
        installation_text = " ".join(
            self._value_or_default(row_map, row_number)
            for row_number in (36, 37, 38, 42, 43, 44)
        )
        supply_advance = sum(self._leading_payment_percentages(self._value_or_default(row_map, 35), marker="advance payment"))
        installation_advance = sum(
            self._leading_payment_percentages(self._value_or_default(row_map, 36), marker="advance payment")
        )
        progressive_installation = self._max_percentage(self._value_or_default(row_map, 38))
        retention = self._max_percentage(" ".join([supply_text, installation_text]), keyword="retention")

        if supply_advance >= 10 and installation_advance >= 10 and progressive_installation >= 70 and retention <= 10:
            allocation = 1.0
            band = "Strong advance plus milestone payment structure"
        elif supply_advance >= 5 and progressive_installation >= 70 and retention <= 10:
            allocation = 0.875
            band = "Healthy payment structure with some milestone dependence"
        elif supply_advance > 0 or installation_advance > 0:
            allocation = 0.75
            band = "Moderate advance and milestone-based recovery"
        elif retention >= 15:
            allocation = 0.25
            band = "Heavy retention and back-ended cash flow"
        elif retention >= 10:
            allocation = 0.375
            band = "Retention-heavy payment structure"
        else:
            allocation = 0.5
            band = "Balanced but contractor-funded payment structure"

        return self._criterion(
            row_number=27,
            point="Payment terms",
            weight=0.2,
            allocation=allocation,
            selected_band=band,
            rationale=(
                f"Supply advance about {supply_advance:.0f}% and installation advance about {installation_advance:.0f}% "
                f"with installation progressive payment about {progressive_installation:.0f}%."
            ),
            source="Appendix-1 payment terms (pages 394-400).",
        )

    def _evaluate_lumpsum_quantities(self, row_map: dict[int, dict[str, Any]], full_text: str) -> EvaluationCriterion:
        contract_type = self._value_or_default(row_map, 9).lower()
        has_unit_rate = "unit rate tender" in full_text
        unit_rate_not_applicable = "schedule of unit rate (not applicable)" in full_text

        if has_unit_rate and not unit_rate_not_applicable:
            allocation = 1.0
            band = "Unit rate and quantified BOQ"
            rationale = "Tender structure indicates a quantified or unit-rate BOQ."
        elif "epc" in contract_type or unit_rate_not_applicable:
            allocation = 0.0
            band = "Lumpsum quantities in supply and civil"
            rationale = "EPC package with schedule of unit rate marked not applicable suggests design-build lump-sum exposure."
        else:
            allocation = 0.5
            band = "Partly lump-sum quantity exposure"
            rationale = "Tender does not expose a full unit-rate BOQ and appears partly quantity-risk based."

        return self._criterion(
            row_number=37,
            point="Lumpsum quantities",
            weight=0.1,
            allocation=allocation,
            selected_band=band,
            rationale=rationale,
            source="Price schedules / EPC structure.",
        )

    def _evaluate_funding(self, row_map: dict[int, dict[str, Any]]) -> EvaluationCriterion:
        funding = self._value_or_default(row_map, 6).lower()
        employer = self._value_or_default(row_map, 5).lower()

        if any(keyword in funding for keyword in ("world bank", "adb", "jica", "foreign", "international")):
            allocation = 1.0
            band = "Foreign or international funding"
            rationale = f"Funding line explicitly states: {self._value_or_default(row_map, 6)}."
            inferred = False
        elif any(keyword in employer for keyword in CENTRAL_UTILITY_KEYWORDS):
            allocation = 0.75
            band = "Central utility"
            rationale = f"Employer is {self._value_or_default(row_map, 5)}, a central-sector utility style employer."
            inferred = True
        elif any(keyword in employer for keyword in PUBLIC_ENTITY_KEYWORDS):
            allocation = 0.5
            band = "State utility or public authority"
            rationale = f"Employer is {self._value_or_default(row_map, 5)}, a public-sector or utility style authority."
            inferred = True
        else:
            allocation = 0.5
            band = "State utility or mixed funding"
            rationale = f"Funding states {self._value_or_default(row_map, 6)} without an international financing clause."
            inferred = True

        return self._criterion(
            row_number=41,
            point="Funding",
            weight=0.1,
            allocation=allocation,
            selected_band=band,
            rationale=rationale,
            source="IFB funding clause / employer identity.",
            inferred=inferred,
        )

    def _evaluate_price_variation(self, row_map: dict[int, dict[str, Any]]) -> EvaluationCriterion:
        price_variation = self._value_or_default(row_map, 47).lower()

        if "firm price" in price_variation or "not applicable" in price_variation:
            allocation = 0.1
            band = "Firm prices"
        elif "20%" in price_variation and "allowed" in price_variation:
            allocation = 1.0
            band = "Allowed for >= 20% of contract value"
        else:
            allocation = 0.5
            band = "Limited price variation"

        return self._criterion(
            row_number=45,
            point="Price variation",
            weight=0.1,
            allocation=allocation,
            selected_band=band,
            rationale=self._value_or_default(row_map, 47),
            source="SCC clause on price adjustment.",
        )

    def _evaluate_completion_period(self, row_map: dict[int, dict[str, Any]]) -> EvaluationCriterion:
        completion_text = self._value_or_default(row_map, 23)
        months = self._first_number(completion_text)
        if months > 18:
            allocation = 1.0
            band = "Very much achievable (>18 months)"
        elif months >= 12:
            allocation = 0.8
            band = "Achievable (12-18 months)"
        else:
            allocation = 0.5
            band = "Tight timeline (<12 months)"

        return self._criterion(
            row_number=49,
            point="Completion period",
            weight=0.05,
            allocation=allocation,
            selected_band=band,
            rationale=f"Completion period extracted as {completion_text}.",
            source="BDS completion schedule.",
        )

    def _evaluate_liquidated_damages(self, full_text: str) -> EvaluationCriterion:
        rate = self._first_decimal_match(full_text, r"liquidated damaged at the rate of\s*([0-9.]+)%")
        maximum = self._first_decimal_match(full_text, r"subject to a maximum of\s*([0-9.]+)%")

        if rate <= 0.5 and maximum <= 5:
            allocation = 1.0
            band = "0.5% per week capped at 5%"
        else:
            allocation = 0.5
            band = "Higher LD exposure"

        return self._criterion(
            row_number=53,
            point="Liquidated damages",
            weight=0.1,
            allocation=allocation,
            selected_band=band,
            rationale=f"LD appears at about {rate}% per week with a maximum near {maximum}%.",
            source="SCC LD clause (pages 229-230).",
        )

    def _evaluate_customer(self, row_map: dict[int, dict[str, Any]]) -> EvaluationCriterion:
        employer = self._value_or_default(row_map, 5).lower()
        if any(keyword in employer for keyword in CENTRAL_UTILITY_KEYWORDS + PUBLIC_ENTITY_KEYWORDS):
            allocation = 1.0
            band = "Preferred"
            rationale = f"Employer name indicates a large public-sector utility: {self._value_or_default(row_map, 5)}."
        else:
            allocation = 0.5
            band = "Not preferred / unknown"
            rationale = f"Employer preference is not explicit in the tender; treated as non-preferred or unknown: {self._value_or_default(row_map, 5)}."

        return self._criterion(
            row_number=56,
            point="Customer",
            weight=0.02,
            allocation=allocation,
            selected_band=band,
            rationale=rationale,
            source="Employer identity in IFB.",
            inferred=True,
        )

    def _evaluate_contractual_risk(self, full_text: str) -> EvaluationCriterion:
        high_risk_markers = (
            "crossfall breach clause",
            "risk and the cost of contractor",
            "bank guarantee",
            "forfeited",
            "terminate the other contracts also",
        )
        hits = sum(1 for marker in high_risk_markers if marker in full_text)

        if hits >= 3:
            allocation = 0.0
            band = "Highly one-sided"
        elif hits >= 1:
            allocation = 0.5
            band = "Moderate risk"
        else:
            allocation = 1.0
            band = "Low or balanced risk"

        return self._criterion(
            row_number=59,
            point="Contractual Risk Allocation",
            weight=0.03,
            allocation=allocation,
            selected_band=band,
            rationale="Cross-fall breach, termination-at-contractor-risk, and multiple security obligations drive the risk view.",
            source="SCC risk allocation clauses.",
            inferred=True,
        )

    def _evaluate_performance_security(self, full_text: str) -> EvaluationCriterion:
        percentage = self._first_decimal_match(
            full_text,
            r"value of contract performance security:[\s\S]+?ten percent\s*\(([\d.]+)%\)",
            default=10.0,
        )

        if percentage <= 5:
            allocation = 1.0
            band = "<=5%"
        elif percentage <= 7.5:
            allocation = 0.75
            band = "5-7.5%"
        else:
            allocation = 0.5
            band = "10% or higher"

        return self._criterion(
            row_number=63,
            point="Performance Security & Guarantees",
            weight=0.05,
            allocation=allocation,
            selected_band=band,
            rationale=f"Contract performance security is around {percentage}% of contract price for the main contracts.",
            source="SCC performance security clause (page 224).",
        )

    def _evaluate_technical_flexibility(self, full_text: str) -> EvaluationCriterion:
        if any(marker in full_text for marker in ("approved make", "specific make", "only from")):
            allocation = 0.8
            band = "Specific make or named-source bias"
            rationale = "Tender text shows make-specific or source-restricted language."
        elif any(marker in full_text for marker in ("iec", "ieee", "is or/and iec")):
            allocation = 1.0
            band = "Open standards-based specification"
            rationale = "Technical clauses point to standards-based compliance without brand-lock language."
        else:
            allocation = 0.9
            band = "Partial flexibility"
            rationale = "Technical flexibility is not fully explicit, but no hard brand lock was detected."

        return self._criterion(
            row_number=67,
            point="Technical Specifications Flexibility",
            weight=0.05,
            allocation=allocation,
            selected_band=band,
            rationale=rationale,
            source="Technical specification sections.",
            inferred=True,
        )

    def _evaluate_site_readiness(self, full_text: str) -> EvaluationCriterion:
        positives = sum(
            1
            for marker in ("existing 11 kv station bus", "abvtps", "power supply and water supply", "existing")
            if marker in full_text
        )
        negatives = sum(
            1
            for marker in ("site-grading", "clearing of vegetation", "bathymetry", "geo-technical", "site clearance")
            if marker in full_text
        )

        if positives >= 2 and negatives == 0:
            allocation = 1.0
            band = "Fully ready"
        elif positives >= 2 and negatives >= 1:
            allocation = 0.8
            band = "Partially ready"
        elif positives >= 1:
            allocation = 0.9
            band = "Mostly ready"
        else:
            allocation = 0.7
            band = "Not fully ready"

        return self._criterion(
            row_number=71,
            point="Project Site Readiness",
            weight=0.03,
            allocation=allocation,
            selected_band=band,
            rationale="Existing project infrastructure is referenced, but surveys, site grading, and related readiness work are still in scope.",
            source="IFB scope and milestone schedule.",
            inferred=True,
        )

    def _evaluate_payment_track_record(self, row_map: dict[int, dict[str, Any]]) -> EvaluationCriterion:
        employer = self._value_or_default(row_map, 5).lower()
        funding = self._value_or_default(row_map, 6).lower()

        if any(keyword in funding for keyword in ("world bank", "adb", "jica")):
            allocation = 1.0
            band = "Excellent"
        elif any(keyword in employer for keyword in CENTRAL_UTILITY_KEYWORDS):
            allocation = 0.75
            band = "Good"
        elif any(keyword in employer for keyword in ("state", "discom", "corporation", "jal nigam", "nigam", "authority", "board", "department", "municipal")):
            allocation = 0.5
            band = "Moderate"
        else:
            allocation = 0.25
            band = "Poor or unknown"

        return self._criterion(
            row_number=76,
            point="Employer's Past Payment Track Record",
            weight=0.02,
            allocation=allocation,
            selected_band=band,
            rationale="Tender does not state payment history explicitly; rating inferred from employer type and funding profile.",
            source="Employer profile / funding clause.",
            inferred=True,
        )

    def _evaluate_joint_venture(self, full_text: str) -> EvaluationCriterion:
        if "whether joint ventures are permitted : no" in full_text and "whether consortium permitted : no" in full_text:
            allocation = 1.0
            band = "Solo qualified bidder"
            rationale = "Tender does not permit bidder JV or consortium participation."
        elif "lead partner" in full_text:
            allocation = 0.9
            band = "JV with clear lead partner"
            rationale = "JV/consortium is allowed with a visible lead-partner construct."
        else:
            allocation = 0.7
            band = "JV with unclear role split"
            rationale = "JV conditions are present without a clear lead-partner construct."

        return self._criterion(
            row_number=82,
            point="Joint Venture / Consortium",
            weight=0.05,
            allocation=allocation,
            selected_band=band,
            rationale=rationale,
            source="BDS bidder structure clause (page 82).",
        )

    def _evaluate_emd(self, row_map: dict[int, dict[str, Any]]) -> EvaluationCriterion:
        emd_form = self._value_or_default(row_map, 13).lower()
        if "bank guarantee" in emd_form:
            allocation = 1.0
            band = "Bank guarantee available"
        elif "eft" in emd_form or "rtgs" in emd_form or "neft" in emd_form:
            allocation = 0.9
            band = "EFT / NEFT / RTGS"
        elif "demand draft" in emd_form:
            allocation = 0.8
            band = "Demand draft"
        elif "insurance surety bond" in emd_form:
            allocation = 0.7
            band = "Insurance surety bond"
        else:
            allocation = 0.8
            band = "Mixed or unspecified mode"

        return self._criterion(
            row_number=86,
            point="EMD",
            weight=0.05,
            allocation=allocation,
            selected_band=band,
            rationale=self._value_or_default(row_map, 13),
            source="BDS bid security form clause.",
        )

    def _evaluate_site_conditions(self, full_text: str) -> EvaluationCriterion:
        hard_markers = ("forest", "eco-sensitive", "high altitude", "remote high-altitude")
        medium_markers = ("bathymetry", "site-grading", "clearing of vegetation", "temporary utilities")
        positive_markers = ("abvtps", "existing 11 kv station bus", "power supply and water supply")

        if any(marker in full_text for marker in hard_markers):
            allocation = 0.25
            band = "Difficult site logistics"
        elif any(marker in full_text for marker in medium_markers) and any(marker in full_text for marker in positive_markers):
            allocation = 0.75
            band = "Manageable existing site with some logistics work"
        elif any(marker in full_text for marker in positive_markers):
            allocation = 1.0
            band = "Accessible site with utilities"
        else:
            allocation = 0.5
            band = "Average site access and utilities"

        return self._criterion(
            row_number=91,
            point="Site Conditions & Accessibility",
            weight=0.05,
            allocation=allocation,
            selected_band=band,
            rationale="Existing plant/grid interface and utility references improve access, but the floating-solar scope still includes survey and site-preparation work.",
            source="IFB scope / technical schedule.",
            inferred=True,
        )

    def _criterion(
        self,
        row_number: int,
        point: str,
        weight: float,
        allocation: float,
        selected_band: str,
        rationale: str,
        source: str,
        inferred: bool = False,
    ) -> EvaluationCriterion:
        weighted_score = round(weight * allocation, 6)
        return EvaluationCriterion(
            row_number=row_number,
            point=point,
            weight=weight,
            allocation=allocation,
            selected_band=selected_band,
            weighted_score=weighted_score,
            rationale=rationale,
            source=source,
            inferred=inferred,
        )

    def _categorize(self, total_fraction: float) -> tuple[str, str, int]:
        if total_fraction >= 0.70:
            return "Category A", "Must bag", 101
        if total_fraction >= 0.56:
            return "Category B", "Target", 102
        if total_fraction >= 0.41:
            return "Category C", "Just bid", 103
        return "Category D", "Not targetted", 104

    def _build_row_map(self, rows: object) -> dict[int, dict[str, Any]]:
        if not isinstance(rows, list):
            return {}
        output: dict[int, dict[str, Any]] = {}
        for row in rows:
            if isinstance(row, dict) and "row_number" in row:
                output[int(row["row_number"])] = row
        return output

    def _value_or_default(self, row_map: dict[int, dict[str, Any]], row_number: int) -> str:
        row = row_map.get(row_number, {})
        value = row.get("value", NOT_AVAILABLE) if isinstance(row, dict) else NOT_AVAILABLE
        if not value:
            return NOT_AVAILABLE
        return str(value)

    def _all_percentages(self, text: str) -> list[float]:
        if not text or text == NOT_AVAILABLE:
            return []
        values: list[float] = []
        for match in re.findall(r"(\d+(?:\.\d+)?)\s*%", text):
            try:
                values.append(float(match))
            except ValueError:
                continue
        return values

    def _leading_payment_percentages(self, text: str, marker: str) -> list[float]:
        if not text or text == NOT_AVAILABLE:
            return []
        values: list[float] = []
        for segment in re.split(r";\s*", text):
            normalized_segment = segment.lower()
            if marker.lower() not in normalized_segment:
                continue
            match = re.search(r"(\d+(?:\.\d+)?)\s*%", segment)
            if match is None:
                continue
            try:
                values.append(float(match.group(1)))
            except ValueError:
                continue
        return values

    def _max_percentage(self, text: str, keyword: str | None = None) -> float:
        if not text or text == NOT_AVAILABLE:
            return 0.0
        search_text = text
        if keyword and keyword.lower() not in search_text.lower():
            return 0.0
        percentages = self._all_percentages(search_text)
        return max(percentages, default=0.0)

    def _first_number(self, text: str) -> float:
        match = re.search(r"(\d+(?:\.\d+)?)", text)
        return float(match.group(1)) if match else 0.0

    def _first_decimal_match(self, text: str, pattern: str, default: float = 0.0) -> float:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if match is None:
            return default
        try:
            return float(match.group(1))
        except (TypeError, ValueError):
            return default

    def _clean_work_title(self, value: str) -> str:
        cleaned = re.split(r"\s+SECTION\s*-\s*[IVX, ]+", value, maxsplit=1, flags=re.IGNORECASE)[0].strip()
        cleaned = re.sub(r"\bBIDDING DOCUMENT NO\.?.*$", "", cleaned, flags=re.IGNORECASE).strip()
        return cleaned or value

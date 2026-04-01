from __future__ import annotations

from dataclasses import dataclass

NOT_AVAILABLE = "Not Available"


@dataclass(frozen=True, slots=True)
class FieldConfig:
    row_number: int
    label: str
    aliases: tuple[str, ...] = ()
    regex_patterns: tuple[str, ...] = ()
    preferred_sections: tuple[str, ...] = ()
    prefer_first_pages: bool = False


FIELD_ROWS: tuple[tuple[int, str], ...] = (
    (4, "Tender Specification No./Tender ID/Tender No."),
    (5, "Name of the Purchaser/Employer/Owner"),
    (6, "Funding Agency"),
    (7, "Name of the work"),
    (8, "Scope of work"),
    (9, "Type of Contract"),
    (10, "Place of the work"),
    (11, "EMD/Bid Security Value (in INR)"),
    (12, "EMD/Bid Security Validity"),
    (13, "Form of EMD"),
    (14, "Bid Validity"),
    (15, "Estimated Total Project Cost (in INR)"),
    (16, "E-Bid Processing Fee (in INR)"),
    (17, "Form of E-Bid Processing Fee"),
    (18, "Cost of Bidding Document (in INR)"),
    (19, "Form of Cost of Bidding Document"),
    (20, "Bid submission date"),
    (21, "Hard Copy Submission"),
    (22, "Techno Commercial Opening"),
    (23, "Completion Period"),
    (24, "Power of Attorney"),
    (25, "Integrity Pact"),
    (26, "Performance Security"),
    (27, "Bank details of Employer"),
    (28, "Qualification Requirement/Qualification Criteria/Eligibility Criteria"),
    (29, "Technical Qualification/Criteria"),
    (30, "Financial Qualification/Criteria"),
    (31, "Net worth"),
    (32, "Average yearly Turnover"),
    (33, "Liquid Assets/Working Capital"),
    (34, "Payment Terms for Supply Portion"),
    (35, "Advance (Supply)"),
    (36, "Advance (Erection)"),
    (37, "Rate of Interest on Advance"),
    (38, "Progressive Payment"),
    (39, "1st Installment (Supply)"),
    (40, "2nd Installment (Supply)"),
    (41, "Final Installment (Supply)"),
    (42, "1st Installment (Erection)"),
    (43, "2nd Installment (Erection)"),
    (44, "Final Installment (Erection)"),
    (45, "Defects Liability Period"),
    (46, "Latent Defect Waranty Period"),
    (47, "Price Variation"),
    (48, "Quantity Variation"),
    (49, "Liquidity Damages/LD"),
    (50, "Taxes and Duties"),
    (51, "Surplus Material"),
    (52, "Contractor's Responsibilities"),
    (53, "Employer's Responsibilities"),
    (54, "Insurance"),
    (55, "Force Majeure"),
    (56, "Correspondance Address"),
    (57, "Client Communication Details"),
    (58, "Tender Uploading Help / assistance"),
    (59, "Any Other T&C"),
)


FIELD_HINTS: dict[str, dict[str, tuple[str, ...] | bool]] = {
    "Tender Specification No./Tender ID/Tender No.": {
        "aliases": ("bidding document no", "tender no", "tender id", "specification no", "nit no", "tender notice no"),
        "regex_patterns": (
            r"BIDDING DOCUMENT(?: NO\.?| NUMBER)?\s*[:\-]\s*([A-Z0-9][A-Z0-9\-_/]+)",
            r"TENDER(?: ID| NO\.?)\s*[:\-]\s*([A-Z0-9][A-Z0-9\-_/]+)",
            r"NIT\s*No\.?\s*[:\-]?\s*([^\n]+)",
            r"Tender\s+Notice\s+No\.?\s*[:\-]?\s*([^\n]+)",
        ),
        "prefer_first_pages": True,
    },
    "Name of the Purchaser/Employer/Owner": {
        "aliases": ("employer", "purchaser", "owner"),
        "regex_patterns": (
            r"^[ \t]*([A-Z][A-Z &().,\-]{8,}LIMITED)\s*$",
            r"^[ \t]*([A-Z][A-Z &().,\-]{8,}(?:NIGAM|AUTHORITY|BOARD|CORPORATION|DEPARTMENT|MINISTRY|MUNICIPALITY|COUNCIL))\s*$",
            r"Employer\s*[:\-]\s*(.+)",
        ),
        "prefer_first_pages": True,
    },
    "Funding Agency": {
        "aliases": ("funding agency", "financing agency", "funded by"),
        "regex_patterns": (
            r"intends to finance the subject package through\s+(.+?)\.",
        ),
        "prefer_first_pages": True,
    },
    "Name of the work": {
        "aliases": ("name of work", "project title", "for development of", "for the work of"),
        "regex_patterns": (
            r"BIDDING DOCUMENTS\s+FOR\s+(.+?)\s+BIDDING DOCUMENT NO",
            r"FOR\s+(.+?)\s+SECTION\s*[-:]",
            r"FOR\s+“?(.+?)”?\s+\(Domestic Competitive Bidding\)",
            r"Name\s+of\s+Work\s*[:\-]\s*([^\n]+)",
            r"For\s+the\s+work\s+of\s*[:\-]?\s*([^\n]+)",
        ),
        "prefer_first_pages": True,
    },
    "Scope of work": {
        "aliases": ("scope of work", "scope", "works to be performed"),
        "regex_patterns": (
            r"2\.1\s+(.+?)(?:2\.2|3\.0)",
        ),
        "prefer_first_pages": True,
    },
    "Type of Contract": {
        "aliases": ("type of contract", "contract type", "epc"),
        "regex_patterns": (
            r"\b(EPC)\b",
            r"Single Stage Two Envelope",
        ),
    },
    "Place of the work": {
        "aliases": ("place of work", "project site", "site location", "at abvtps"),
        "regex_patterns": (
            r"\bat\s+(ABVTPS Project of CSPGCL)\b",
        ),
        "prefer_first_pages": True,
    },
    "EMD/Bid Security Value (in INR)": {
        "aliases": ("bid security amount", "emd", "earnest money deposit", "bid security"),
        "regex_patterns": (
            r"Amount of Bid\s*Security\s*:\s*(INR[0-9,./ \-]+(?:only)?)",
            r"Bid Security for an amount of\s*(INR\s*[0-9.]+\s*Crore\s*\(INR[^)]+\))",
        ),
        "preferred_sections": ("Section III", "Section I"),
    },
    "EMD/Bid Security Validity": {
        "aliases": ("bid security validity", "emd validity", "validity of bid security"),
        "regex_patterns": (
            r"Period of validity of Bid Security\s*:\s*([^.\n]+)",
            r"Bid Security shall remain valid for a period of\s+(.+?)\.",
        ),
        "preferred_sections": ("Section III",),
    },
    "Form of EMD": {
        "aliases": (
            "form of bid security",
            "form of emd",
            "bank guarantee",
            "insurance surety bond",
            "payment on order instrument",
        ),
        "regex_patterns": (
            r"Bid Security shall, at the Bidder'?s option, be in the form of\s+(.+?)\.",
        ),
        "preferred_sections": ("Section III",),
    },
    "Bid Validity": {
        "aliases": ("bid validity", "validity of bid"),
        "regex_patterns": (
            r"Bids shall remain valid for\s+(.+?)\s+from the closing date prescribed by Employer",
        ),
        "preferred_sections": ("Section III",),
    },
    "Estimated Total Project Cost (in INR)": {
        "aliases": ("estimated project cost", "total project cost", "estimated cost"),
    },
    "E-Bid Processing Fee (in INR)": {
        "aliases": ("e-bid processing fee", "processing fee"),
    },
    "Form of E-Bid Processing Fee": {
        "aliases": ("form of e-bid processing fee", "form of processing fee"),
    },
    "Cost of Bidding Document (in INR)": {
        "aliases": ("cost of bidding document", "cost of tender document", "tender fee"),
        "regex_patterns": (
            r"Cost of Bidding Documents in INR\s+(Not Applicable)",
            r"Cost of Documents in INR\s+(Not Applicable)",
        ),
        "prefer_first_pages": True,
    },
    "Form of Cost of Bidding Document": {
        "aliases": ("form of cost of bidding document", "form of tender fee"),
        "regex_patterns": (
            r"Cost of Bidding Documents in INR\s+(Not Applicable)",
            r"Cost of Documents in INR\s+(Not Applicable)",
        ),
        "prefer_first_pages": True,
    },
    "Bid submission date": {
        "aliases": ("last date and time for submission of bid", "bid submission date", "deadline for submission"),
        "regex_patterns": (
            r"Last Date and Time for receipt of bids comprising\s+both Techno-Commercial Bid and Price Bid\s+([0-9.]+\s+[0-9:]+\s*Hrs\.?)",
            r"Deadline for Bid Submission on the date and time as stated in\s+(.+?)\.",
        ),
        "preferred_sections": ("Section I", "Section III"),
        "prefer_first_pages": True,
    },
    "Hard Copy Submission": {
        "aliases": ("hard copy submission", "physical submission", "offline submission"),
        "regex_patterns": (
            r"Submission of Bid Security, Joint Deed of Undertaking.*?Power of Attorney in Physical Form\.",
            r"Bids shall be submitted online\. Only Bid Security, notarized Power of Attorney, and Pass Phrases are to be submitted in original hard copy at the following address\.",
        ),
        "preferred_sections": ("Section III",),
    },
    "Techno Commercial Opening": {
        "aliases": ("techno commercial opening", "opening of techno commercial bid", "bid opening"),
        "regex_patterns": (
            r"Date\s*&\s*Time of opening of Techno-Commercial\s+Bid\s+([0-9.]+\s+[0-9:]+\s*Hrs\.?)",
            r"Date and Time for Techno-Commercial Bid Opening:\s+(.+?)\.",
        ),
        "preferred_sections": ("Section I", "Section III"),
        "prefer_first_pages": True,
    },
    "Completion Period": {
        "aliases": ("completion period", "time for completion", "completion schedule"),
        "regex_patterns": (
            r"Time for Completion of Facilities from the date of Notification\s+of Award:\s*([^.\n]+)",
        ),
        "preferred_sections": ("Section III",),
    },
    "Power of Attorney": {
        "aliases": ("power of attorney",),
        "regex_patterns": (
            r"The bidders are also requested to submit Power of Attorney, duly notarized in hard copy as per provisions of bidding documents in a separate sealed envelope.*",
            r"Power of Attorney.*?submitted offline in original in a separate sealed envelope\.",
        ),
        "preferred_sections": ("Section III",),
        "prefer_first_pages": True,
    },
    "Integrity Pact": {
        "aliases": ("integrity pact",),
        "regex_patterns": (
            r"Integrity Pact shall be submitted by the bidder.*?e-tendering portal.*",
            r"Do you Commit to all the provisions of the Integrity\s+Pact\?",
        ),
        "preferred_sections": ("Section III", "Section VII"),
    },
    "Performance Security": {
        "aliases": ("performance security", "performance bank guarantee"),
        "regex_patterns": (
            r"The performance security shall be denominated in the currency of the Contract and shall be\s+(.+?)\.",
            r"The Contractor shall, within twenty-eight \(28\) days of the Notification of Award, provide securities for the due performance of the Contract.*",
        ),
        "preferred_sections": ("Section IV", "Section VII"),
    },
    "Bank details of Employer": {
        "aliases": ("bank details of employer", "bank details", "employer bank"),
        "regex_patterns": (
            r"Bank Name:\s*Axis Bank[\s\S]+?IFSC Code:\s*[A-Z0-9]+",
        ),
        "preferred_sections": ("Section III",),
    },
    "Qualification Requirement/Qualification Criteria/Eligibility Criteria": {
        "aliases": ("qualification requirements", "qualification criteria", "eligibility criteria"),
        "regex_patterns": (
            r"QUALIFYING REQUIREMENTS FOR BIDDERS",
            r"In addition to the requirements stipulated.*?Clause 1\.0 and Clause 2\.0",
        ),
        "prefer_first_pages": True,
    },
    "Technical Qualification/Criteria": {
        "aliases": ("technical qualification", "technical criteria", "technical experience"),
        "regex_patterns": (
            r"1\.0 TECHNICAL CRITERIA",
            r"1\.1 The Bidder should have designed, supplied, erected/ supervised erection and commissioned/[\s\S]+?date of techno-commercial bid opening\.",
        ),
        "prefer_first_pages": True,
    },
    "Financial Qualification/Criteria": {
        "aliases": ("financial qualification", "financial criteria", "financial requirement"),
        "regex_patterns": (
            r"2\.0 FINANCIAL CRITERIA",
            r"2\.1 The average annual turnover of the Bidder, should not be less than[\s\S]+?techno-commercial bid opening\.",
        ),
        "prefer_first_pages": True,
    },
    "Net worth": {
        "aliases": ("net worth",),
        "regex_patterns": (
            r"2\.2 Net Worth of the Bidder[\s\S]+?their respective paid-up share capitals\.",
        ),
        "prefer_first_pages": True,
    },
    "Average yearly Turnover": {
        "aliases": ("average annual turnover", "average yearly turnover"),
        "regex_patterns": (
            r"2\.1 The average annual turnover of the Bidder, should not be less than\s+(.+?)\.",
        ),
        "prefer_first_pages": True,
    },
    "Liquid Assets/Working Capital": {
        "aliases": ("liquid assets", "working capital"),
    },
    "Payment Terms for Supply Portion": {
        "aliases": ("payment terms for supply portion", "supply payment terms"),
        "regex_patterns": (
            r"TERMS OF PAYMENT[\s\S]+?A\. Schedule No\.1: Plant and Equipment.*?In respect of Plant and Equipment supplied from within the Employer's country the following payment shall be made:",
        ),
    },
    "Advance (Supply)": {
        "aliases": ("advance supply", "advance payment supply"),
        "regex_patterns": (
            r"Fifteen Percent\s*\(15\s*%\)\s*of the total Ex-Works price component as Initial Advance Payment",
        ),
    },
    "Advance (Erection)": {
        "aliases": ("advance erection", "advance payment erection", "installation advance"),
        "regex_patterns": (
            r"Five Percent\s*\(5\s*%\)\s*of the Installation Services Component\s*\(excluding AMC\)\s*of the Contract Price will be paid to the Contractor[\s\S]+?advance payment on",
        ),
    },
    "Rate of Interest on Advance": {
        "aliases": ("rate of interest on advance", "interest on advance"),
        "regex_patterns": (
            r"bearing interest at the rate of\s+(.+?100 bps\]\s*per annum)",
        ),
    },
    "Progressive Payment": {
        "aliases": ("progressive payment", "milestone payment"),
        "regex_patterns": (
            r"Seventy-Five Percent\s*\(75\s*%\)\s*of the installation Services component of contract price shall be paid on pro-rata basis[\s\S]+?billed\.",
        ),
    },
    "1st Installment (Supply)": {
        "aliases": ("1st installment supply", "first installment supply"),
        "regex_patterns": (
            r"Fifty Five Percent\s*\(55\s*%\)\s*of Ex-works price component of the Contract price for each identified equipment upon dispatch of equipment[\s\S]+?representative\.",
        ),
    },
    "2nd Installment (Supply)": {
        "aliases": ("2nd installment supply", "second installment supply"),
        "regex_patterns": (
            r"Fifteen Percent\s*\(15\s*%\)\s*of Ex-works price component of the Contract Price for each identified equipment on receipt of equipment at site[\s\S]+?site\.",
        ),
    },
    "Final Installment (Supply)": {
        "aliases": ("final installment supply",),
        "regex_patterns": (
            r"Ten Percent\s*\(10\s*%\)\s*of Ex-works price component of the Contract price on successful completion of Performance Guarantee tests[\s\S]+?Operational Acceptance Certificate\.",
        ),
    },
    "1st Installment (Erection)": {
        "aliases": ("1st installment erection", "first installment erection"),
        "regex_patterns": (
            r"Seventy-Five Percent\s*\(75\s*%\)\s*of the installation Services component of contract price shall be paid on pro-rata basis[\s\S]+?billed\.",
        ),
    },
    "2nd Installment (Erection)": {
        "aliases": ("2nd installment erection", "second installment erection"),
        "regex_patterns": (
            r"Two Point Five Percent\s*\(2\.5%\)\s*of total Installation price of the Contract shall be paid on successful commissioning of part capacity[\s\S]+?part commissioning\.",
        ),
    },
    "Final Installment (Erection)": {
        "aliases": ("final installment erection",),
        "regex_patterns": (
            r"Ten Percent\s*\(10\s*%\)\s*of total Installation price of the Contract shall be paid on successful completion of Performance Guarantee Tests[\s\S]+?Operational Acceptance Certificate\.",
        ),
    },
    "Defects Liability Period": {
        "aliases": ("defects liability period", "defect liability period"),
        "regex_patterns": (
            r"twenty one \(21\) months after Completion of the Facilities.*?or fifteen \(15\) months after Operational Acceptance.*?whichever occurs first",
        ),
    },
    "Latent Defect Waranty Period": {
        "aliases": ("latent defect warranty period", "latent defect waranty period"),
        "regex_patterns": (
            r"latent defects warranty shall be limited to a period of\s+(.+?)\.",
        ),
    },
    "Price Variation": {
        "aliases": ("price variation", "price adjustment"),
        "regex_patterns": (
            r"Provision of Price Adjustment is not applicable[\s\S]+?Firm Price Basis[\s\S]+?remain Firm during entire period of contract\.",
        ),
    },
    "Quantity Variation": {
        "aliases": ("quantity variation", "variation in quantity"),
    },
    "Liquidity Damages/LD": {
        "aliases": ("liquidated damages", "ld", "liquidity damages"),
        "regex_patterns": (
            r"the Contractor shall pay to the Employer liquidated damages in the amount computed at the rates specified in the SCC.*",
        ),
    },
    "Taxes and Duties": {
        "aliases": ("taxes and duties", "gst", "duties and taxes"),
        "regex_patterns": (
            r"Except as otherwise specifically provided in the Contract, the Contractor shall bear and pay all taxes, duties, levies and charges.*",
            r"100% of applicable Taxes and Duties which are payable by the Employer under the Contract shall be paid/reimbursed.*",
        ),
    },
    "Surplus Material": {
        "aliases": ("surplus material", "scrap and surplus material"),
        "regex_patterns": (
            r"Ownership of any Plant and Equipment in excess of the requirements for the Facilities.*?surplus material.*?shall revert to the Contractor.*",
        ),
    },
    "Contractor's Responsibilities": {
        "aliases": ("contractor responsibilities", "responsibilities of contractor"),
        "regex_patterns": (
            r"The Contractor shall be responsible for the true and proper setting-out of the Facilities.*",
            r"Unless otherwise provided in the Contract, the Contractor shall be responsible for the recruitment, transportation, accommodation and catering of all labour.*",
        ),
    },
    "Employer's Responsibilities": {
        "aliases": ("employer responsibilities", "responsibilities of employer"),
        "regex_patterns": (
            r"The Employer shall ensure the accuracy of all information and/or data to be supplied by the Employer.*",
        ),
    },
    "Insurance": {
        "aliases": ("insurance",),
        "regex_patterns": (
            r"The Insurance Policy shall be valid for a minimum period of twenty-five \(25\) years.*",
            r"submission of documentary evidence by the Contractor towards having taken the insurance policy\(ies\).*",
        ),
    },
    "Force Majeure": {
        "aliases": ("force majeure",),
        "regex_patterns": (
            r"Force Majeure.+?any event beyond the reasonable control of the Employer or of the Contractor.*",
        ),
    },
    "Correspondance Address": {
        "aliases": ("correspondence address", "correspondance address", "address for communication"),
        "regex_patterns": (
            r"11\.0 ADDRESS FOR COMMUNICATION[\s\S]+?E-Mail:.*",
        ),
        "prefer_first_pages": True,
    },
    "Client Communication Details": {
        "aliases": ("client communication details", "communication details", "contact details"),
        "regex_patterns": (
            r"Telephone No\.,\s*.+?E-Mail:\s*.+",
            r"Telephone No\.\s*.+?E-mail:\s*.+",
        ),
        "prefer_first_pages": True,
    },
    "Tender Uploading Help / assistance": {
        "aliases": ("tender uploading help", "assistance", "help desk", "e-tendering assistance"),
        "regex_patterns": (
            r"For technical assistance, interested parties may call ETS Helpdesk at\s+(.+?)\.",
            r"M/s ISN Electronic Tender Services \(ETS\) will provide all necessary training and assistance.*",
        ),
        "prefer_first_pages": True,
    },
    "Any Other T&C": {
        "aliases": ("other terms and conditions", "special conditions", "other t and c"),
    },
}


FIELD_CONFIGS: tuple[FieldConfig, ...] = tuple(
    FieldConfig(
        row_number=row_number,
        label=label,
        aliases=tuple(FIELD_HINTS.get(label, {}).get("aliases", ())),  # type: ignore[arg-type]
        regex_patterns=tuple(FIELD_HINTS.get(label, {}).get("regex_patterns", ())),  # type: ignore[arg-type]
        preferred_sections=tuple(FIELD_HINTS.get(label, {}).get("preferred_sections", ())),  # type: ignore[arg-type]
        prefer_first_pages=bool(FIELD_HINTS.get(label, {}).get("prefer_first_pages", False)),
    )
    for row_number, label in FIELD_ROWS
)

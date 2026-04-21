"""Generate the 10_seed.sql file for the assessment database.

Run this to regenerate seed data. Output is deterministic (fixed seed) so
re-running produces identical SQL. The generated file is committed to the
repo; this script is shipped alongside it for reproducibility.

Usage:
    python3 db/generate_seed.py > db/init/10_seed.sql
"""

from __future__ import annotations

import random
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta

SEED = 20260420
random.seed(SEED)

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

STREET_NAMES = [
    "Main", "Market", "Elm", "Oak", "Maple", "Cedar", "Pine", "Washington",
    "Jefferson", "Madison", "Lincoln", "Park", "Lake", "Hill", "River",
    "Commerce", "Industrial", "Corporate", "Technology", "Innovation",
    "Research", "Broadway", "Pennsylvania", "Constitution", "Liberty",
    "Franklin", "Harrison", "Monroe", "Jackson", "Roosevelt",
]

STREET_TYPE_FULL = {
    "St": "Street", "Ave": "Avenue", "Blvd": "Boulevard", "Rd": "Road",
    "Dr": "Drive", "Ln": "Lane", "Pkwy": "Parkway", "Pl": "Place",
    "Ct": "Court", "Way": "Way",
}

CITIES = {
    "CA": ["Los Angeles", "San Francisco", "San Diego", "San Jose", "Sacramento", "Palo Alto", "Mountain View"],
    "NY": ["New York", "Buffalo", "Rochester", "Albany", "Syracuse"],
    "TX": ["Houston", "Dallas", "Austin", "San Antonio", "Fort Worth", "Plano"],
    "DE": ["Wilmington", "Dover", "Newark"],
    "IL": ["Chicago", "Springfield", "Naperville"],
    "MA": ["Boston", "Cambridge", "Waltham", "Burlington"],
    "FL": ["Miami", "Tampa", "Orlando", "Jacksonville"],
    "WA": ["Seattle", "Redmond", "Bellevue", "Spokane"],
    "GA": ["Atlanta", "Savannah", "Augusta"],
    "CO": ["Denver", "Boulder", "Colorado Springs"],
    "OH": ["Columbus", "Cleveland", "Cincinnati"],
    "NC": ["Charlotte", "Raleigh", "Durham"],
    "NJ": ["Newark", "Jersey City", "Princeton"],
    "PA": ["Philadelphia", "Pittsburgh", "Harrisburg"],
    "VA": ["Arlington", "Richmond", "Norfolk", "Reston"],
    "MI": ["Detroit", "Ann Arbor", "Grand Rapids"],
    "MN": ["Minneapolis", "Saint Paul", "Rochester"],
    "AZ": ["Phoenix", "Tucson", "Scottsdale"],
    "MD": ["Baltimore", "Bethesda", "Annapolis"],
    "CT": ["Hartford", "Stamford", "New Haven"],
}

SIC_CODES = ["7372", "7371", "2834", "3674", "6199", "4813", "5812", "3711", "2911", "6020", "3812", "7389", "5961", "4512"]

# -- Core entity definitions -----------------------------------------------------
# Each core entity is a real-world company that will appear across 2 or 3
# sources with name and address variations. The `variants` dict specifies how
# the name is written in each source. `alt_names` are historical or DBA names
# used to add further noise.

@dataclass
class CoreEntity:
    base_name: str
    sec_name: str
    state_name: str
    usasp_name: str
    street_num: int
    street_name: str
    street_type_abbrev: str
    suite: str
    state: str
    zip5: str
    zip_plus4: str
    sic: str
    ticker: str | None
    state_of_inc: str
    entity_type: str
    sources: tuple[str, ...] = ("sec", "state", "usaspending")
    extra_state_jurisdictions: tuple[str, ...] = field(default_factory=tuple)
    # Overrides for deliberate test scenarios (shell address, shared agent,
    # parent/subsidiary). Leave None/False to use the randomized defaults.
    force_agent_name: str | None = None
    force_agent_street: str | None = None
    usasp_subsidiary_name: str | None = None
    fixed_sources: bool = False


CORES: list[CoreEntity] = [
    CoreEntity("Acme Semiconductor", "ACME SEMICONDUCTOR CORP",     "Acme Semiconductor Corporation",        "Acme Semiconductor Corp.",
               1200, "Innovation", "Dr",   "Suite 400",  "CA", "95054", "95054-1234", "3674", "ACMS", "DE", "Corporation", extra_state_jurisdictions=("CA",)),
    CoreEntity("Baker & Holt Industries", "BAKER AND HOLT INDUSTRIES INC", "Baker & Holt Industries, Inc.", "Baker Holt Industries",
               45,   "Commerce",   "St",   "",           "OH", "44114", "44114-2210", "3812", "BKHT", "OH", "Corporation"),
    CoreEntity("Cascade Biotech", "CASCADE BIOTECH INC",            "Cascade Biotech, Inc.",                 "CASCADE BIOTECH",
               780,  "Research",   "Pkwy", "Building C", "WA", "98052", "98052-6301", "2834", "CSCB", "DE", "Corporation", extra_state_jurisdictions=("WA",)),
    CoreEntity("Delta Logistics", "DELTA LOGISTICS LLC",            "Delta Logistics, L.L.C.",               "Delta Logistics LLC",
               9090, "Industrial", "Blvd", "Ste 200",    "TX", "75019", "75019-0100", "4512", None,   "TX", "LLC"),
    CoreEntity("Evergreen Capital Partners", "EVERGREEN CAPITAL PARTNERS LP", "Evergreen Capital Partners, L.P.", "Evergreen Capital Partners",
               500,  "Park",       "Ave",  "Floor 32",   "NY", "10022", "10022-0500", "6199", None,   "DE", "Limited Partnership", extra_state_jurisdictions=("NY",)),
    CoreEntity("Fulton Pharmaceuticals", "FULTON PHARMACEUTICALS, INC.", "FULTON PHARMACEUTICALS INC",        "Fulton Pharma",
               22,   "Technology", "Way",  "",           "NJ", "08540", "08540-3366", "2834", "FLTN", "DE", "Corporation"),
    CoreEntity("Granite Ridge Energy", "GRANITE RIDGE ENERGY CORP",  "Granite Ridge Energy Corporation",      "Granite Ridge Energy",
               10000, "Corporate", "Dr",  "",           "TX", "77056", "77056-0010", "2911", "GRID", "DE", "Corporation"),
    CoreEntity("Harbor Point Financial", "HARBOR POINT FINANCIAL GROUP INC", "Harbor Point Financial Group, Inc.", "Harbor Point Fincl Grp",
               1,    "Harbor",     "Pl",   "",           "MA", "02110", "02110-0001", "6020", "HPFG", "DE", "Corporation", extra_state_jurisdictions=("MA",)),
    CoreEntity("IronClad Security",         "IRONCLAD SECURITY SERVICES INC",    "Ironclad Security Services, Inc.", "IRONCLAD SECURITY",
               2200, "Liberty",    "Ave",  "Suite 100",  "VA", "22102", "22102-2200", "7389", None,   "VA", "Corporation"),
    CoreEntity("Jasper Medical Devices",    "JASPER MEDICAL DEVICES INC",        "Jasper Medical Devices, Inc.",     "Jasper Medical",
               55,   "Jefferson",  "St",   "",           "MN", "55401", "55401-0055", "3841", "JMED", "MN", "Corporation"),
    CoreEntity("Kestrel Aerospace",         "KESTREL AEROSPACE HOLDINGS INC",    "Kestrel Aerospace Holdings, Inc.", "Kestrel Aerospace",
               3030, "Constitution","Blvd","",           "AZ", "85034", "85034-3030", "3721", "KSAE", "DE", "Corporation"),
    CoreEntity("Lumen Valley Software",     "LUMEN VALLEY SOFTWARE INC",         "Lumen Valley Software, Inc.",      "Lumen Valley Software Inc",
               100,  "Innovation", "Way",  "",           "CA", "94043", "94043-0100", "7372", "LUMV", "DE", "Corporation", extra_state_jurisdictions=("CA",)),
    CoreEntity("Meridian Food Services",    "MERIDIAN FOOD SERVICES LLC",        "Meridian Food Services, LLC",      "Meridian Food Svcs",
               780,  "Commerce",   "Ct",   "",           "GA", "30303", "30303-0780", "5812", None,   "GA", "LLC"),
    CoreEntity("Nimbus Cloud Networks",     "NIMBUS CLOUD NETWORKS INC",         "Nimbus Cloud Networks, Inc.",      "Nimbus Cloud Networks",
               200,  "Technology", "Pkwy", "Building 2", "WA", "98004", "98004-0200", "7371", "NMBS", "DE", "Corporation"),
    CoreEntity("Orion Precision Motors",    "ORION PRECISION MOTORS CORP",       "Orion Precision Motors Corporation","Orion Precision Motors",
               48000,"Industrial", "Dr",   "",           "MI", "48176", "48176-4800", "3711", "ORPM", "DE", "Corporation", extra_state_jurisdictions=("MI",)),
    CoreEntity("Piedmont Mutual Insurance", "PIEDMONT MUTUAL INSURANCE COMPANY", "Piedmont Mutual Insurance Co.",    "Piedmont Mutual Ins Co",
               8,    "Independence","Blvd","",           "NC", "28202", "28202-0008", "6311", None,   "NC", "Mutual Insurance Company"),
    CoreEntity("Quantum Ridge Robotics",    "QUANTUM RIDGE ROBOTICS INC",        "Quantum Ridge Robotics, Inc.",     "Quantum Ridge Robotics",
               14,   "Research",   "Pkwy", "",           "MA", "02142", "02142-0014", "3812", "QRRB", "DE", "Corporation"),
    CoreEntity("Redwood Analytics",         "REDWOOD ANALYTICS INC",             "Redwood Analytics, Inc.",          "Redwood Analytics",
               600,  "Main",       "St",   "Suite 900",  "CA", "94111", "94111-0600", "7372", "RWAN", "DE", "Corporation", extra_state_jurisdictions=("CA",)),
    CoreEntity("Sterling Brothers Construction", "STERLING BROS CONSTRUCTION INC", "Sterling Brothers Construction, Inc.", "STERLING BROTHERS CONSTRUCTION",
               7700, "Jackson",    "Ave",  "",           "IL", "60601", "60601-7700", "1541", None,   "IL", "Corporation"),
    CoreEntity("Tidewater Marine Services", "TIDEWATER MARINE SERVICES LLC",     "Tidewater Marine Services, L.L.C.","Tidewater Marine Svcs",
               101,  "Harbor",     "Rd",   "",           "LA", "70112", "70112-0101", "4412", None,   "LA", "LLC"),
    CoreEntity("Union Peak Resources",      "UNION PEAK RESOURCES CORP",         "Union Peak Resources Corporation", "Union Peak Resources",
               555,  "Lincoln",    "St",   "Suite 2000", "CO", "80203", "80203-0555", "1000", "UNPK", "DE", "Corporation"),
    CoreEntity("Vanguard Diagnostics",      "VANGUARD DIAGNOSTICS INCORPORATED", "Vanguard Diagnostics, Inc.",       "Vanguard Diagnostics",
               4400, "Pennsylvania","Ave", "",           "MD", "20814", "20814-4400", "8071", "VGDI", "MD", "Corporation"),
    CoreEntity("Westbrook Textiles",        "WESTBROOK TEXTILES INC",            "Westbrook Textiles, Inc.",         "Westbrook Textiles",
               25,   "Maple",      "St",   "",           "SC", "29301", "29301-0025", "2211", None,   "SC", "Corporation"),
    CoreEntity("Xenith Biomedical",         "XENITH BIOMEDICAL CORP",            "Xenith Biomedical Corporation",    "Xenith Biomedical",
               12,   "Research",   "Dr",   "",           "CA", "92121", "92121-0012", "2836", "XNBM", "DE", "Corporation"),
    CoreEntity("Yarrow Agricultural",       "YARROW AGRICULTURAL HOLDINGS LLC",  "Yarrow Agricultural Holdings, LLC","Yarrow Agricultural Holdings",
               3300, "River",      "Rd",   "",           "IA", "50309", "50309-3300", "0111", None,   "IA", "LLC"),
    CoreEntity("Zephyr Wireless",           "ZEPHYR WIRELESS COMMUNICATIONS INC","Zephyr Wireless Communications, Inc.","Zephyr Wireless Comms",
               800,  "Market",     "St",   "Floor 40",   "CA", "94103", "94103-0800", "4813", "ZWCI", "DE", "Corporation"),
    CoreEntity("Archer & Finch Advisors",   "ARCHER & FINCH ADVISORS LLC",       "Archer and Finch Advisors, LLC",   "Archer Finch Advisors",
               11,   "Broadway",   "",     "Suite 2100", "NY", "10004", "10004-0011", "6282", None,   "DE", "LLC"),
    # Dept / Department abbreviation coverage.
    CoreEntity("Beacon Federal Dept Services", "BEACON FEDERAL DEPT SERVICES INC", "Beacon Federal Department Services, Inc.", "BEACON FEDERAL DEPARTMENT SERVICES",
               1800, "Constitution","Ave", "",           "VA", "22202", "22202-1800", "7389", None,   "VA", "Corporation",
               sources=("sec", "state", "usaspending"), fixed_sources=True, extra_state_jurisdictions=("DC", "MD")),
    CoreEntity("Copperleaf Energy Storage", "COPPERLEAF ENERGY STORAGE INC",     "Copperleaf Energy Storage, Inc.",  "Copperleaf Energy Storage",
               65,   "Innovation", "Ln",   "",           "CO", "80301", "80301-0065", "3691", "CLES", "DE", "Corporation"),
    CoreEntity("Driftwood Hospitality",     "DRIFTWOOD HOSPITALITY GROUP LLC",   "Driftwood Hospitality Group, LLC", "Driftwood Hospitality Grp",
               18,   "Ocean",      "Blvd", "",           "FL", "33139", "33139-0018", "7011", None,   "FL", "LLC", extra_state_jurisdictions=("NV",)),
    CoreEntity("Emberline Publishing",      "EMBERLINE PUBLISHING INC",          "Emberline Publishing, Inc.",       "Emberline Publishing",
               500,  "Madison",    "Ave",  "",           "NY", "10022", "10022-0501", "2731", None,   "NY", "Corporation"),
    CoreEntity("Falcon Crest Realty",       "FALCON CREST REALTY TRUST",         "Falcon Crest Realty Trust",        "Falcon Crest Realty",
               2,    "Lake",       "Dr",   "",           "IL", "60601", "60601-0002", "6798", "FCRT", "MD", "Real Estate Investment Trust", extra_state_jurisdictions=("IL",)),
    CoreEntity("Gilded Oak Vineyards",      "GILDED OAK VINEYARDS INC",          "Gilded Oak Vineyards, Inc.",       "Gilded Oak Vineyards",
               7540, "Vineyard",   "Ln",   "",           "CA", "94558", "94558-7540", "2084", None,   "CA", "Corporation"),
    CoreEntity("Hollyfield Media",          "HOLLYFIELD MEDIA HOLDINGS INC",     "Hollyfield Media Holdings, Inc.",  "Hollyfield Media",
               9000, "Sunset",     "Blvd", "Floor 15",   "CA", "90028", "90028-9000", "2711", "HFMH", "DE", "Corporation"),
    CoreEntity("Inverness Shipping",        "INVERNESS SHIPPING CO",             "Inverness Shipping Company",       "Inverness Shipping",
               44,   "Pier",       "",     "",           "WA", "98101", "98101-0044", "4412", None,   "WA", "Corporation"),
    CoreEntity("Juniper Hollow Farms",      "JUNIPER HOLLOW FARMS COOPERATIVE",  "Juniper Hollow Farms, A Cooperative","Juniper Hollow Farms",
               3100, "Farm",       "Rd",   "",           "WI", "53703", "53703-3100", "0112", None,   "WI", "Cooperative"),
    CoreEntity("Keystone Automotive Parts", "KEYSTONE AUTOMOTIVE PARTS INC",     "Keystone Automotive Parts, Inc.",  "Keystone Auto Parts",
               18,   "Industrial", "Dr",   "",           "PA", "17101", "17101-0018", "5013", None,   "PA", "Corporation"),
    # Univ / University abbreviation coverage.
    CoreEntity("Prairie University Press",  "PRAIRIE UNIV PRESS INC",            "Prairie University Press, Inc.",   "Prairie Univ Press",
               75,   "School",     "St",   "",           "MA", "02108", "02108-0075", "2731", None,   "MA", "Corporation",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    CoreEntity("Monarch Bridge Builders",   "MONARCH BRIDGE BUILDERS LLC",       "Monarch Bridge Builders, LLC",     "Monarch Bridge Builders",
               2050, "Construction","Way", "",           "TX", "78701", "78701-2050", "1622", None,   "TX", "LLC"),
    CoreEntity("Northwind Chemical",        "NORTHWIND CHEMICAL COMPANY",        "Northwind Chemical Co.",           "Northwind Chemical",
               900,  "Chemical",   "Dr",   "",           "LA", "70113", "70113-0900", "2800", "NWCH", "LA", "Corporation"),
    CoreEntity("Oakhaven Retirement Group", "OAKHAVEN RETIREMENT GROUP INC",     "Oakhaven Retirement Group, Inc.",  "Oakhaven Retirement",
               100,  "Oak",        "Ln",   "",           "FL", "33411", "33411-0100", "8051", None,   "FL", "Corporation"),
    CoreEntity("Providence Scientific",     "PROVIDENCE SCIENTIFIC INSTRUMENTS INC", "Providence Scientific Instruments, Inc.", "Providence Scientific Instr",
               16,   "Research",   "Pl",   "",           "RI", "02903", "02903-0016", "3827", "PSCI", "RI", "Corporation"),
    CoreEntity("Quintessa Apparel",         "QUINTESSA APPAREL GROUP INC",       "Quintessa Apparel Group, Inc.",    "Quintessa Apparel",
               550,  "Fashion",    "Ave",  "Floor 7",    "NY", "10018", "10018-0550", "2300", "QAPR", "NY", "Corporation"),
    CoreEntity("Rockwater Engineering",     "ROCKWATER ENGINEERING INC",         "Rockwater Engineering, Inc.",      "Rockwater Engineering",
               6100, "Engineering","Dr",   "",           "TX", "77024", "77024-6100", "8711", None,   "TX", "Corporation"),
    # St / Saint abbreviation coverage.
    CoreEntity("Saint Mary Pediatric Care", "ST MARY PEDIATRIC CARE INC",        "Saint Mary Pediatric Care, Inc.",  "St Mary Pediatric",
               444,  "Hospital",   "Dr",   "",           "OH", "45202", "45202-0444", "8060", None,   "OH", "Corporation",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    CoreEntity("Thornbury Public Relations","THORNBURY PUBLIC RELATIONS LLC",    "Thornbury Public Relations, LLC",  "Thornbury PR",
               330,  "Monroe",     "St",   "Suite 1100", "IL", "60606", "60606-0330", "8743", None,   "IL", "LLC"),
    CoreEntity("Underwood Defense",         "UNDERWOOD DEFENSE SYSTEMS INC",     "Underwood Defense Systems, Inc.",  "UNDERWOOD DEFENSE SYSTEMS",
               1500, "Pentagon",   "Rd",   "",           "VA", "22209", "22209-1500", "3812", "UWDS", "VA", "Corporation", extra_state_jurisdictions=("CA", "TX")),
    CoreEntity("Vista Grande Resorts",      "VISTA GRANDE RESORTS INC",          "Vista Grande Resorts, Inc.",       "Vista Grande Resorts",
               9,    "Canyon",     "Rd",   "",           "NV", "89109", "89109-0009", "7011", "VGRR", "NV", "Corporation"),
    CoreEntity("Wavelength Audio",          "WAVELENGTH AUDIO HOLDINGS INC",     "Wavelength Audio Holdings, Inc.",  "Wavelength Audio",
               2,    "Studio",     "Pl",   "",           "CA", "90028", "90028-0002", "3651", None,   "DE", "Corporation"),
    CoreEntity("Xavier & Stone Legal",      "XAVIER & STONE LEGAL SERVICES PC",  "Xavier and Stone Legal Services, P.C.", "Xavier Stone Legal",
               1000, "Court",      "St",   "Floor 20",   "DC", "20001", "20001-1000", "8111", None,   "DC", "Professional Corporation"),
    CoreEntity("Yellowfield Agronomics",    "YELLOWFIELD AGRONOMICS INC",        "Yellowfield Agronomics, Inc.",     "Yellowfield Agronomics",
               4545, "Field",      "Rd",   "",           "KS", "66603", "66603-4545", "0721", None,   "KS", "Corporation"),
    CoreEntity("Zenithal Exploration",      "ZENITHAL EXPLORATION LLC",          "Zenithal Exploration, LLC",        "Zenithal Exploration",
               77,   "Summit",     "Dr",   "",           "UT", "84111", "84111-0077", "1311", None,   "UT", "LLC"),
    # Two near-duplicate entities — deliberately ambiguous (different companies, very similar names)
    CoreEntity("Atlantic Coastal Freight",  "ATLANTIC COASTAL FREIGHT CO",       "Atlantic Coastal Freight Company", "Atlantic Coastal Freight",
               200,  "Harbor",     "Blvd", "",           "NJ", "07302", "07302-0200", "4213", None,   "NJ", "Corporation"),
    CoreEntity("Atlantic Coast Freight",    "ATLANTIC COAST FREIGHT LLC",        "Atlantic Coast Freight, LLC",      "Atlantic Coast Freight",
               88,   "Shore",      "Rd",   "",           "NC", "28401", "28401-0088", "4213", None,   "NC", "LLC"),
    # Same-name-different-entity collision (franchise / common name)
    CoreEntity("Summit Consulting Group (DC)", "SUMMIT CONSULTING GROUP INC",    "Summit Consulting Group, Inc.",    "Summit Consulting Group",
               700,  "K",          "St",   "Suite 400",  "DC", "20006", "20006-0700", "8742", None,   "DC", "Corporation"),
    CoreEntity("Summit Consulting Group (CO)", "SUMMIT CONSULTING GROUP LLC",    "Summit Consulting Group, LLC",     "Summit Consulting Group",
               450,  "Mountain",   "Rd",   "",           "CO", "80202", "80202-0450", "8742", None,   "CO", "LLC"),
    # Company that changed its name (historical variant)
    CoreEntity("Helios Photovoltaic",       "HELIOS PHOTOVOLTAIC INC",           "Helios Photovoltaic, Inc. (formerly SunStar Solar, Inc.)", "Helios Photovoltaic",
               900,  "Solar",      "Way",  "",           "AZ", "85281", "85281-0900", "3674", "HLPV", "DE", "Corporation"),
    # Acronym-heavy name (tests expansion/handling)
    CoreEntity("NTI Systems Integration",   "N T I SYSTEMS INTEGRATION INC",     "NTI Systems Integration, Inc.",    "NTI Sys Integration",
               3300, "Technology", "Dr",   "Building A", "MD", "20878", "20878-3300", "7389", None,   "MD", "Corporation"),
    # DBA heavy
    CoreEntity("Greenway Organic Foods",    "GREENWAY ORGANIC FOODS INC",        "Greenway Organic Foods, Inc. DBA Greenway Market", "Greenway Market",
               42,   "Harvest",    "Ln",   "",           "OR", "97201", "97201-0042", "5411", None,   "OR", "Corporation"),
    # Scenario 1: shell-address collision. Two unrelated entities both registered
    # at Corporation Trust Company's well-known Delaware service address. Any
    # matcher that weights address equality heavily will false-positive here.
    CoreEntity("Magnolia Tax Advisory",     "MAGNOLIA TAX ADVISORY SERVICES INC","Magnolia Tax Advisory Services, Inc.", "Magnolia Tax Advisory",
               1209, "N Orange",   "St",   "",           "DE", "19801", "19801-1120", "8931", None,   "DE", "Corporation",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    CoreEntity("Verity Global Imports",     "VERITY GLOBAL IMPORTS LLC",         "Verity Global Imports, LLC",       "Verity Global Imports",
               1209, "N Orange",   "St",   "",           "DE", "19801", "19801-1120", "5099", None,   "DE", "LLC",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    # Scenario 2: shared registered agent. Unrelated entities with different
    # operational addresses but identical CT Corporation System agent name
    # and service address. Matching on agent fields alone will false-positive.
    CoreEntity("Halberd Analytics",         "HALBERD ANALYTICS INC",             "Halberd Analytics, Inc.",          "Halberd Analytics",
               4400, "Technology", "Dr",   "Suite 500",  "GA", "30303", "30303-4400", "7372", None,   "DE", "Corporation",
               force_agent_name="CT Corporation System", force_agent_street="251 Little Falls Drive",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    CoreEntity("Ironbark Pet Supplies",     "IRONBARK PET SUPPLIES CORP",        "Ironbark Pet Supplies Corporation","Ironbark Pet Supplies",
               88,   "Market",     "St",   "",           "OR", "97201", "97201-0088", "5961", None,   "DE", "Corporation",
               force_agent_name="CT Corporation System", force_agent_street="251 Little Falls Drive",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    # Scenario 4: parent / subsidiary. SEC and state rows carry the parent's
    # name; USAspending contracts are awarded to the federal-services
    # subsidiary with the parent captured in parent_name. Linking the
    # subsidiary row to the parent entity requires reading parent_name.
    CoreEntity("Riverstone Holdings",       "RIVERSTONE HOLDINGS CORP",          "Riverstone Holdings Corporation",  "Riverstone Holdings Corp.",
               1285, "Avenue of the Americas", "", "Floor 38", "NY", "10019", "10019-6028", "6199", "RSTH", "DE", "Corporation",
               usasp_subsidiary_name="Riverstone Federal Programs LLC",
               sources=("sec", "state", "usaspending"), fixed_sources=True, extra_state_jurisdictions=("NY",)),
    # Scenario 6: numeric / series entities. Real-estate LP structures that
    # share name stem, address, and registered agent. Distinguished only by
    # the roman-numeral suffix.
    CoreEntity("Westpoint Industrial Realty II",  "WESTPOINT INDUSTRIAL REALTY II LP",  "Westpoint Industrial Realty II, L.P.",  "Westpoint Industrial Realty II",
               400,  "Madison",    "Ave",  "Suite 1200", "NY", "10017", "10017-0400", "6798", None,   "DE", "Limited Partnership",
               force_agent_name="CT Corporation System",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    CoreEntity("Westpoint Industrial Realty III", "WESTPOINT INDUSTRIAL REALTY III LP", "Westpoint Industrial Realty III, L.P.", "Westpoint Industrial Realty III",
               400,  "Madison",    "Ave",  "Suite 1200", "NY", "10017", "10017-0400", "6798", None,   "DE", "Limited Partnership",
               force_agent_name="CT Corporation System",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    # Founder-name variation: Joe vs Joseph nickname, and a middle initial
    # that is spelled out in one source and dropped in another. All three
    # rows refer to the same firm and the same founder.
    CoreEntity("Joseph P. Martinez Consulting", "JOSEPH P MARTINEZ CONSULTING INC", "Joe Martinez Consulting, Inc.", "Joseph Martinez Consulting",
               1200, "Commerce",   "St",   "Suite 310",  "TX", "75201", "75201-1200", "8742", None,   "TX", "Corporation",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    # Hyphenated given name variation: Emily-Rose appears hyphenated in
    # SEC, space-separated in state, and with "Rose" demoted to a middle
    # initial in USAspending.
    CoreEntity("Emily-Rose Callaway Studio", "EMILY-ROSE CALLAWAY STUDIO LLC",    "Emily Rose Callaway Studio, LLC",  "Emily R. Callaway Studio",
               250,  "Art",        "Ave",  "",           "NY", "10011", "10011-0250", "7389", None,   "NY", "LLC",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
    # Department / division variation: the parent is the registered entity
    # in SEC. State names the parent with a d/b/a division label. USAspending
    # contracts are awarded to a differently-worded version of the same
    # division with the parent captured in parent_name.
    CoreEntity("Cresthaven Logistics",       "CRESTHAVEN LOGISTICS INC",          "Cresthaven Logistics, Inc. d/b/a Cresthaven Federal Division", "Cresthaven Logistics Corp.",
               2200, "Harbor",     "Blvd", "",           "FL", "33101", "33101-2200", "4213", None,   "DE", "Corporation",
               usasp_subsidiary_name="Cresthaven Logistics - Federal Services Division",
               sources=("sec", "state", "usaspending"), fixed_sources=True),
]

# Drop a few from each source randomly so not every core entity appears everywhere
# (tests that candidate does not assume full coverage)
def assign_sources(rng: random.Random) -> None:
    for ent in CORES:
        if ent.fixed_sources:
            continue
        r = rng.random()
        if r < 0.70:
            ent.sources = ("sec", "state", "usaspending")
        elif r < 0.85:
            ent.sources = ("sec", "state")
        elif r < 0.93:
            ent.sources = ("state", "usaspending")
        else:
            ent.sources = ("sec", "usaspending")

rng = random.Random(SEED)
assign_sources(rng)

# -- Name / address variation helpers -------------------------------------------

def vary_name_case(name: str, style: str) -> str:
    if style == "upper":
        return name.upper()
    if style == "title":
        return name.title().replace(" Inc.", ", Inc.").replace(" Llc", ", LLC")
    return name


def add_comma_before_suffix(name: str) -> str:
    for suffix in (" INC", " LLC", " CORP", " LP", " CO", " CORPORATION"):
        low = name.upper()
        idx = low.rfind(suffix)
        if idx != -1 and (idx == 0 or name[idx-1] != ","):
            return name[:idx] + "," + name[idx:]
    return name


def drop_punctuation(name: str) -> str:
    return name.replace(",", "").replace(".", "")


def expand_street_type(street: str) -> str:
    parts = street.rsplit(" ", 1)
    if len(parts) == 2 and parts[1] in STREET_TYPE_FULL:
        return f"{parts[0]} {STREET_TYPE_FULL[parts[1]]}"
    return street


def abbreviate_suite(suite: str) -> str:
    return suite.replace("Suite", "Ste").replace("Building", "Bldg").replace("Floor", "Fl")


def rand_date(rng: random.Random, start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=rng.randrange(delta))


# -- Record builders -------------------------------------------------------------

@dataclass
class SecRow:
    cik: int
    company_name: str
    ticker: str | None
    sic_code: str
    state_of_inc: str
    street: str
    city: str
    state: str
    zip_code: str
    last_filed_date: date


@dataclass
class StateRow:
    registration_id: str
    jurisdiction: str
    entity_name: str
    entity_type: str
    agent_name: str | None
    agent_street: str
    agent_city: str
    agent_state: str
    agent_zip: str
    formation_date: date
    status: str


@dataclass
class UsaspRow:
    uei: str
    recipient_name: str
    parent_name: str | None
    street: str
    city: str
    state: str
    zip_code: str
    total_awards: float
    last_award_date: date


sec_rows: list[SecRow] = []
state_rows: list[StateRow] = []
usasp_rows: list[UsaspRow] = []

# -- Per-core-entity records -----------------------------------------------------

cik_counter = 1000001
uei_counter = 100001

for ent in CORES:
    base_street = f"{ent.street_num} {ent.street_name} {ent.street_type_abbrev}".strip()
    city = rng.choice(CITIES.get(ent.state, [f"{ent.state} City"]))
    if "sec" in ent.sources:
        sec_street = base_street if rng.random() < 0.5 else expand_street_type(base_street)
        if ent.suite:
            sec_street = f"{sec_street}, {ent.suite}"
        sec_rows.append(SecRow(
            cik=cik_counter,
            company_name=ent.sec_name,
            ticker=ent.ticker,
            sic_code=ent.sic,
            state_of_inc=ent.state_of_inc,
            street=sec_street,
            city=city,
            state=ent.state,
            zip_code=ent.zip_plus4 if rng.random() < 0.5 else ent.zip5,
            last_filed_date=rand_date(rng, date(2023, 1, 1), date(2026, 3, 1)),
        ))
        cik_counter += rng.randrange(1, 50)

    if "state" in ent.sources:
        jurisdictions = (ent.state_of_inc,) + ent.extra_state_jurisdictions
        for j_idx, juris in enumerate(jurisdictions):
            reg_id = f"{juris}-{rng.randrange(100000, 9999999)}"
            name = ent.state_name
            if rng.random() < 0.3:
                name = drop_punctuation(name)
            if rng.random() < 0.2:
                name = name.upper()
            if ent.force_agent_street:
                state_street = ent.force_agent_street
            else:
                state_street = base_street if rng.random() < 0.5 else expand_street_type(base_street)
                if ent.suite and rng.random() < 0.6:
                    state_street = f"{state_street}, {abbreviate_suite(ent.suite)}"
            zip_ = ent.zip5 if rng.random() < 0.6 else ent.zip_plus4
            if ent.force_agent_name:
                agent_name = ent.force_agent_name
            else:
                agent_name = f"CT Corporation System" if rng.random() < 0.3 else f"{ent.base_name.split()[0]} Registered Agent LLC"
            state_rows.append(StateRow(
                registration_id=reg_id,
                jurisdiction=juris,
                entity_name=name,
                entity_type=ent.entity_type,
                agent_name=agent_name,
                agent_street=state_street,
                agent_city=city if rng.random() < 0.4 else rng.choice(CITIES.get(juris, [f"{juris} City"])),
                agent_state=juris,
                agent_zip=zip_,
                formation_date=rand_date(rng, date(1985, 1, 1), date(2022, 1, 1)),
                status=rng.choice(["Active", "Active", "Active", "Active", "Good Standing", "Active"]),
            ))

    if "usaspending" in ent.sources:
        uei = f"UEI{uei_counter:09d}"
        uei_counter += rng.randrange(1, 30)
        if ent.usasp_subsidiary_name:
            name = ent.usasp_subsidiary_name
            parent_name = ent.usasp_name
        else:
            name = ent.usasp_name
            if rng.random() < 0.35:
                name = name.upper()
            parent_name = None if rng.random() < 0.85 else ent.usasp_name
        usasp_street = base_street if rng.random() < 0.5 else expand_street_type(base_street)
        if ent.suite and rng.random() < 0.4:
            usasp_street = f"{usasp_street}, {abbreviate_suite(ent.suite)}"
        usasp_rows.append(UsaspRow(
            uei=uei,
            recipient_name=name,
            parent_name=parent_name,
            street=usasp_street,
            city=city,
            state=ent.state,
            zip_code=ent.zip5 if rng.random() < 0.7 else ent.zip_plus4,
            total_awards=round(rng.uniform(50_000, 50_000_000), 2),
            last_award_date=rand_date(rng, date(2022, 1, 1), date(2026, 3, 1)),
        ))

# -- Noise singletons ------------------------------------------------------------
# Each source has records that do not appear in any other source. Candidates
# should end up with a single entity_source_link for these.

NOISE_PREFIXES = [
    "Alpine", "Birch", "Cobalt", "Dune", "Eclipse", "Fable", "Glacier", "Horizon",
    "Ironwood", "Juno", "Kinetic", "Lantern", "Moss", "Nova", "Obsidian", "Prairie",
    "Quartz", "Rapid", "Sable", "Terrace", "Umbra", "Verdant", "Willow", "Xylem",
    "Yonder", "Zephyr", "Ashford", "Bramble", "Cinder", "Dorado", "Estuary",
    "Flint", "Gravel", "Hazel", "Ibis", "Jovial", "Kindred", "Loam", "Meadow",
]
NOISE_NOUNS = [
    "Holdings", "Partners", "Ventures", "Labs", "Group", "Industries", "Systems",
    "Technologies", "Solutions", "Associates", "Enterprises", "Capital", "Traders",
    "Logistics", "Media", "Retail", "Distributors", "Agency", "Consultancy",
    "Foods", "Metals", "Plastics", "Services", "Manufacturing", "Co",
]

def random_noise_name(rng: random.Random) -> str:
    return f"{rng.choice(NOISE_PREFIXES)} {rng.choice(NOISE_NOUNS)}"

def random_address(rng: random.Random) -> tuple[str, str, str, str]:
    state = rng.choice(list(CITIES.keys()))
    city = rng.choice(CITIES[state])
    street_type = rng.choice(list(STREET_TYPE_FULL.keys()))
    street = f"{rng.randrange(1, 9999)} {rng.choice(STREET_NAMES)} {street_type}"
    if rng.random() < 0.3:
        street = f"{street}, Suite {rng.randrange(100, 2500)}"
    zip5 = f"{rng.randrange(10000, 99999)}"
    return street, city, state, zip5

SEC_SUFFIXES = ["INC", "CORP", "CO", "LLC", "LTD", "LP"]
STATE_TYPES = ["Corporation", "LLC", "Limited Partnership", "Corporation", "Corporation", "LLC"]

# Target counts: 500 total rows. Core fills ~220 records. Need ~280 noise records
# distributed so total per-table counts feel balanced.
target_sec = 170
target_state = 220
target_usasp = 110

used_noise_names: set[str] = set()

def uniq_noise_name(base: str) -> str:
    """Ensure overall uniqueness of noise names (so the candidate's matcher is not tempted by false positives)."""
    candidate = base
    i = 2
    while candidate in used_noise_names:
        candidate = f"{base} {i}"
        i += 1
    used_noise_names.add(candidate)
    return candidate

while len(sec_rows) < target_sec:
    n = uniq_noise_name(random_noise_name(rng))
    name = f"{n.upper()} {rng.choice(SEC_SUFFIXES)}"
    street, city, state, zip5 = random_address(rng)
    sec_rows.append(SecRow(
        cik=cik_counter,
        company_name=name,
        ticker=None if rng.random() < 0.7 else ''.join(rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(4)),
        sic_code=rng.choice(SIC_CODES),
        state_of_inc=rng.choice(["DE", "DE", "DE", state]),
        street=street,
        city=city,
        state=state,
        zip_code=zip5,
        last_filed_date=rand_date(rng, date(2022, 1, 1), date(2026, 3, 1)),
    ))
    cik_counter += rng.randrange(1, 50)

while len(state_rows) < target_state:
    base = random_noise_name(rng)
    n = uniq_noise_name(base)
    etype = rng.choice(STATE_TYPES)
    name = f"{n}, {'Inc.' if etype == 'Corporation' else 'LLC' if etype == 'LLC' else 'L.P.'}"
    street, city, state, zip5 = random_address(rng)
    state_rows.append(StateRow(
        registration_id=f"{state}-{rng.randrange(100000, 9999999)}",
        jurisdiction=state,
        entity_name=name,
        entity_type=etype,
        agent_name="CT Corporation System" if rng.random() < 0.25 else f"{base.split()[0]} Agent Services LLC",
        agent_street=street,
        agent_city=city,
        agent_state=state,
        agent_zip=zip5,
        formation_date=rand_date(rng, date(1980, 1, 1), date(2023, 1, 1)),
        status=rng.choice(["Active", "Active", "Active", "Dissolved", "Good Standing", "Inactive"]),
    ))

while len(usasp_rows) < target_usasp:
    n = uniq_noise_name(random_noise_name(rng))
    street, city, state, zip5 = random_address(rng)
    usasp_rows.append(UsaspRow(
        uei=f"UEI{uei_counter:09d}",
        recipient_name=n,
        parent_name=None,
        street=street,
        city=city,
        state=state,
        zip_code=zip5,
        total_awards=round(rng.uniform(10_000, 10_000_000), 2),
        last_award_date=rand_date(rng, date(2021, 1, 1), date(2026, 3, 1)),
    ))
    uei_counter += rng.randrange(1, 30)

# -- Shuffle so core/noise are interleaved ---------------------------------------
rng.shuffle(sec_rows)
rng.shuffle(state_rows)
rng.shuffle(usasp_rows)

# -- Emit SQL --------------------------------------------------------------------

def esc(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, date):
        return f"'{v.isoformat()}'"
    s = str(v).replace("'", "''")
    return f"'{s}'"


def emit_inserts(table: str, columns: list[str], rows: list[tuple]) -> None:
    print(f"INSERT INTO {table} ({', '.join(columns)}) VALUES")
    lines = [
        "    (" + ", ".join(esc(v) for v in row) + ")"
        for row in rows
    ]
    print(",\n".join(lines) + ";\n")


def main() -> None:
    print("-- iSimcha entity linking assessment: seed data")
    print(f"-- Generated by db/generate_seed.py with seed={SEED}")
    print(f"-- Row counts: sec={len(sec_rows)}, state={len(state_rows)}, usaspending={len(usasp_rows)}, total={len(sec_rows)+len(state_rows)+len(usasp_rows)}")
    print()

    emit_inserts(
        "sources.sec_companies",
        ["cik", "company_name", "ticker", "sic_code", "state_of_inc", "street", "city", "state", "zip_code", "last_filed_date"],
        [(r.cik, r.company_name, r.ticker, r.sic_code, r.state_of_inc, r.street, r.city, r.state, r.zip_code, r.last_filed_date) for r in sec_rows],
    )
    emit_inserts(
        "sources.state_registrations",
        ["registration_id", "jurisdiction", "entity_name", "entity_type", "agent_name", "agent_street", "agent_city", "agent_state", "agent_zip", "formation_date", "status"],
        [(r.registration_id, r.jurisdiction, r.entity_name, r.entity_type, r.agent_name, r.agent_street, r.agent_city, r.agent_state, r.agent_zip, r.formation_date, r.status) for r in state_rows],
    )
    emit_inserts(
        "sources.usaspending_recipients",
        ["uei", "recipient_name", "parent_name", "street", "city", "state", "zip_code", "total_awards", "last_award_date"],
        [(r.uei, r.recipient_name, r.parent_name, r.street, r.city, r.state, r.zip_code, r.total_awards, r.last_award_date) for r in usasp_rows],
    )


if __name__ == "__main__":
    main()

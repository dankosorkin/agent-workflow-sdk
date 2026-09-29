/**
 * Physical Schema for PartyContact MongoDB Database
 * Generated from live database introspection
 * Database: contact
 */

const schema = {
  database: "contact",
  
  collections: {
    /**
     * ATAKL10 - Main contact link table
     * Links customers (SIFRUR_LAKOACH) to their contact addresses by type (SUG_MIVNE_KTOVET)
     * Central hub that references all address-type collections via MISPAR_KTOVET
     */
    ATAKL10: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Unique address identifier
        SIFRUR_LAKOACH: "number",          // Customer identifier
        SUG_MIVNE_KTOVET: "number",        // Address type code (12, 16, 20, 40, 50, 60, 66, 90)
        SUG_KTOVET: "number",              // Address usage type
        contact_stg_key: "string",         // Staging key
        TAARICH10_IDKUN: "date",           // Update date
        TAARICH10_PTICHA: "date",          // Open date
        RAMAT_ADIFUT: "number",            // Priority level
        KOD_KTOVET_MSB: "number",          // Address validity code
        KOD_SIBAT_SBS_KTV: "string",       // Fault address code
        KOD_DOAR_SHIVUKI: "number",
        KOD_KIYUM_PRATIM: "number",
        MAKOR_BE_IMS: "string",            // IMS source
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "SIFRUR_LAKOACH_1_SUG_MIVNE_KTOVET_1", keys: { SIFRUR_LAKOACH: 1, SUG_MIVNE_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 1348790,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAK112 - Local addresses (SUG_MIVNE_KTOVET=12)
     * Contains Israeli address details: city, street, building number, zip codes
     */
    ATAK112: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address identifier (joins to ATAKL10)
        SHEM_YISHUV_TIKNI: "string",       // City name
        SEMEL_YISHUV: "number",            // City code
        SHEM_RHV_MINHALI: "string",        // Street name
        SEMEL_RECHOV: "number",            // Street code
        MPR_BAYIT_MINHALI: "number",       // Building number
        MISPAR_KNISA: "string",            // Entrance number
        MIKUD: "string",                   // 5-digit zip code
        MIKUD_CHADASH: "string",           // 7-digit zip code
        GEO_COORDINATE_X: "number",        // Longitude
        GEO_COORDINATE_Y: "number",        // Latitude
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 600447,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAKL12 - Additional local address fields (1:1 with ATAK112)
     * Contains apartment details, mailbox name, workplace info
     */
    ATAKL12: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address identifier (joins to ATAK112)
        SIFRUR_LAKOACH: "number",          // Customer identifier
        contact_stg_key: "string",
        OT_BE_MISPAR_BAYIT: "string",      // Letter in house number
        MISPAR_KNISA: "string",            // Entrance number
        MPR_DIRA_MINHALI: "number",        // Apartment number
        SHEM_BE_TEVAT_DOAR: "string",      // Name on mailbox
        MEAFYEN_NOSAF: "string",           // Additional notes
        SHEM_MAKOM_AVODA: "string",        // Working place
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 600447,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAK116 - Abroad addresses (SUG_MIVNE_KTOVET=16)
     */
    ATAK116: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",
        KOD_ERETZ: "number",               // Country code
        SHEM_MEDINA: "string",             // State/country name
        SHEM_YISHUV_CHUL: "string",        // City abroad
        SHEM_RHV_MNHL_LAZ: "string",       // Street abroad (Latin)
        MPR_BAYIT_MINHALI: "number",       // Building number
        MIKUD_CHUL: "string",              // Zip code abroad
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 118094,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAK120 - P.O. Box addresses (SUG_MIVNE_KTOVET=20)
     */
    ATAK120: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",
        TA_DOAR: "number",                 // P.O. Box number
        SUG_TA_DOAR: "string",             // P.O. Box type
        SHEM_YISHUV_TIKNI: "string",       // City name
        SEMEL_YISHUV: "number",            // City code
        MIKUD: "string",                   // 5-digit zip
        MIKUD_CHADASH: "string",           // 7-digit zip
        GEO_COORDINATE_X: "number",
        GEO_COORDINATE_Y: "number",
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 68950,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAK140 - P.O. Box in branch (SUG_MIVNE_KTOVET=40)
     */
    ATAK140: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",
        MISPAR_BANK: "number",             // Bank number
        MISPAR_SNIF: "number",             // Branch number
        MPR_TA_DOAR_BASNIF: "number",      // P.O. Box in branch
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 59169,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAK150 - SWIFT addresses (SUG_MIVNE_KTOVET=50)
     */
    ATAK150: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",
        KOD_BANK_SWIFT: "string",          // SWIFT bank code
        KOD_MEDINA_SWIFT: "string",        // SWIFT country code
        KOD_IR_SWIFT: "string",            // SWIFT city code
        YEHIDA_NOSAF_SWIFT: "string",      // SWIFT additional unit
        MPRSNT_O_YCD_O_NSF: "string",
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 88212,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAK160 - Local phones (SUG_MIVNE_KTOVET=60)
     */
    ATAK160: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",
        SUG_KTOVET_TELEPHON: "number",     // Phone type
        KIDOMET_TELEPHON: "string",        // Phone prefix (area code)
        EZOR_CHIUG: "string",              // Dialing area
        MISPAR_TELEPHON: "string",         // Phone number
        KOD_TELEPHON_O_FAX: "number",      // Phone or fax indicator (2/3 = fax)
        MISPAR_IBAN: "string",             // IBAN number
        SUG_TELEPHON: "string",            // Phone subtype
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 197365,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAK166 - Abroad phones (SUG_MIVNE_KTOVET=66)
     */
    ATAK166: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",
        KOD_ERETZ: "number",               // Country code
        KIDOMET_ERETZ_TEL: "string",       // Country phone prefix
        KIDOMET_YISHUV: "string",          // Area prefix
        MPR_TELEPHON_CHUL: "string",       // Phone number abroad
        KOD_TELEPHON_O_FAX: "number",      // Phone or fax indicator
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 88595,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    },

    /**
     * ATAK190 - Email addresses (SUG_MIVNE_KTOVET=90)
     */
    ATAK190: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",
        KTOVET_E_MAIL: "string",           // Email address
        KOD_MARC_MAKOR: "number",
        LOAD_DATE: "date",
        TIMESTAMP_SHINUA_ROW: "date"
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 127958,
      uniqueKey: { fields: ["MISPAR_KTOVET"], source: "inferred", confidence: 95 }
    }
  },

  relationships: [
    // ATAKL10 is the central hub linking to all address-type collections
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAK112", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "Local addresses when SUG_MIVNE_KTOVET=12" },
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAKL12", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "Additional local fields when SUG_MIVNE_KTOVET=12" },
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAK116", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "Abroad addresses when SUG_MIVNE_KTOVET=16" },
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAK120", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "P.O. Box when SUG_MIVNE_KTOVET=20" },
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAK140", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "P.O. Box in branch when SUG_MIVNE_KTOVET=40" },
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAK150", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "SWIFT address when SUG_MIVNE_KTOVET=50" },
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAK160", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "Local phone when SUG_MIVNE_KTOVET=60" },
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAK166", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "Abroad phone when SUG_MIVNE_KTOVET=66" },
    { from: "ATAKL10", fromField: "MISPAR_KTOVET", to: "ATAK190", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "Email when SUG_MIVNE_KTOVET=90" },
    // ATAK112 and ATAKL12 have a 1:1 relationship for local addresses
    { from: "ATAK112", fromField: "MISPAR_KTOVET", to: "ATAKL12", toField: "MISPAR_KTOVET", cardinality: "1:1", note: "Combined local address details" }
  ],

  notes: [
    // Optimization hints for query planning
    "ATAKL10 is the central hub - all customer contact queries should start here",
    "ATAKL10 has compound index on (SIFRUR_LAKOACH, SUG_MIVNE_KTOVET) - efficient for 'get all contacts of type X for customer Y'",
    "All detail collections (ATAK*) have index on MISPAR_KTOVET - supports $lookup joins from ATAKL10",
    "No unique indexes exist - MISPAR_KTOVET uniqueness is inferred from data, not enforced",
    "ATAK112 + ATAKL12 should be joined together for complete local address (same doc count suggests 1:1)",
    "SUG_MIVNE_KTOVET determines which detail collection to join: 12→ATAK112/L12, 16→ATAK116, 20→ATAK120, 40→ATAK140, 50→ATAK150, 60→ATAK160, 66→ATAK166, 90→ATAK190",
    "Fan-out risk: One customer (SIFRUR_LAKOACH) can have many ATAKL10 records (1:N); each ATAKL10 joins 1:1 to exactly one detail collection",
    "For PartyContact synthesis: Start with ATAKL10, filter by SUG_MIVNE_KTOVET, then $lookup to appropriate ATAK* collection",
    "Consider adding unique index on MISPAR_KTOVET in each collection to enforce data integrity"
  ]
};

module.exports = schema;

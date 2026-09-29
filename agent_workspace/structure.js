/**
 * Physical Schema: PartyContact Database
 * Generated from live MongoDB introspection
 * Database: contact
 * 
 * This schema describes the source collections that feed the PartyContact
 * target document, which aggregates various contact types (addresses, phones,
 * emails) for party/customer records.
 */

const schema = {
  database: "contact",
  
  collections: {
    /**
     * ATAKL10 - Contact Header/Link Table
     * Main routing table that links customers (SIFRUR_LAKOACH) to their
     * contact addresses via MISPAR_KTOVET. The SUG_MIVNE_KTOVET field
     * determines which detail table (ATAK1xx) contains the address details.
     */
    ATAKL10: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        SIFRUR_LAKOACH: "number",          // Customer ID
        SUG_MIVNE_KTOVET: "number",        // Address structure type (12,16,20,40,50,60,66,90)
        SUG_KTOVET: "number",              // Address usage type
        TAARICH10_IDKUN: "date",           // Update date
        TAARICH10_PTICHA: "date",          // Open date
        RAMAT_ADIFUT: "number",            // Priority level
        KOD_KTOVET_MSB: "number",          // Invalid address indicator
        KOD_SIBAT_SBS_KTV: "string",       // Invalid address reason code
        KOD_DOAR_SHIVUKI: "number",        // Marketing mail code
        KOD_KIYUM_PRATIM: "number",        // Details existence code
        MAKOR_BE_IMS: "string",            // IMS source
        contact_stg_key: "string",         // Staging key
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string"         // Source table name
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "SIFRUR_LAKOACH_1_SUG_MIVNE_KTOVET_1", keys: { SIFRUR_LAKOACH: 1, SUG_MIVNE_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 1348790,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAKL12 - Local Address Extension
     * Additional fields for local addresses (SUG_MIVNE_KTOVET=12).
     * 1:1 relationship with ATAK112 via MISPAR_KTOVET.
     */
    ATAKL12: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        SIFRUR_LAKOACH: "number",          // Customer ID
        OT_BE_MISPAR_BAYIT: "string",      // Letter in house number
        MISPAR_KNISA: "number",            // Entrance number
        MPR_DIRA_MINHALI: "number",        // Apartment number
        SHEM_BE_TEVAT_DOAR: "string",      // Name on mailbox
        MEAFYEN_NOSAF: "string",           // Additional attribute/notes
        SHEM_MAKOM_AVODA: "string",        // Workplace name
        contact_stg_key: "string",         // Staging key
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string"         // Source table name
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 600447,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAK112 - Local Address Details
     * Core address information for local (Israeli) addresses.
     * SUG_MIVNE_KTOVET=12 in ATAKL10.
     */
    ATAK112: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        SHEM_YISHUV_TIKNI: "string",       // City name (standard)
        SEMEL_YISHUV: "number",            // City code
        SHEM_RHV_MINHALI: "string",        // Street name (administrative)
        SEMEL_RECHOV: "number",            // Street code
        MPR_BAYIT_MINHALI: "number",       // Building number
        MISPAR_KNISA: "number",            // Entrance number
        MIKUD_CHADASH: "string",           // New zip code (7 digits)
        MIKUD: "string",                   // Old zip code (5 digits)
        GEO_COORDINATE_X: "number",        // Geo coordinate X
        GEO_COORDINATE_Y: "number",        // Geo coordinate Y
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string"         // Source table name
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 600447,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAK116 - Abroad Address Details
     * Address information for international addresses.
     * SUG_MIVNE_KTOVET=16 in ATAKL10.
     */
    ATAK116: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        KOD_ERETZ: "number",               // Country code
        SHEM_MEDINA: "string",             // State/province name
        SHEM_YISHUV_CHUL: "string",        // City name (abroad)
        SHEM_RHV_MNHL_LAZ: "string",       // Street name (Latin chars)
        MPR_BAYIT_MINHALI: "number",       // Building number
        MIKUD_CHUL: "string",              // Foreign zip code
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string"         // Source table name
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 118094,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAK120 - P.O. Box Address
     * Post office box address details.
     * SUG_MIVNE_KTOVET=20 in ATAKL10.
     */
    ATAK120: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        TA_DOAR: "number",                 // P.O. Box number
        SUG_TA_DOAR: "string",             // P.O. Box type code
        SHEM_YISHUV_TIKNI: "string",       // City name
        SEMEL_YISHUV: "number",            // City code
        MIKUD_CHADASH: "string",           // New zip code
        MIKUD: "string",                   // Old zip code
        GEO_COORDINATE_X: "number",        // Geo coordinate X
        GEO_COORDINATE_Y: "number",        // Geo coordinate Y
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string"         // Source table name
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 68950,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAK140 - P.O. Box in Bank Branch
     * P.O. Box located at a bank branch.
     * SUG_MIVNE_KTOVET=40 in ATAKL10.
     */
    ATAK140: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        MISPAR_BANK: "number",             // Bank number
        MISPAR_SNIF: "number",             // Branch number
        MPR_TA_DOAR_BASNIF: "number",      // P.O. Box number at branch
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string",        // Source table name
        SIFRUR_CDS_LE_ISKA: "string"       // CDS transaction code
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 59169,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAK150 - SWIFT Address
     * SWIFT banking address for international transfers.
     * SUG_MIVNE_KTOVET=50 in ATAKL10.
     */
    ATAK150: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        KOD_BANK_SWIFT: "string",          // SWIFT bank code
        KOD_MEDINA_SWIFT: "string",        // SWIFT country code
        KOD_IR_SWIFT: "string",            // SWIFT city code
        YEHIDA_NOSAF_SWIFT: "string",      // SWIFT additional unit
        MPRSNT_O_YCD_O_NSF: "string",      // Representative/unit/additional
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string",        // Source table name
        SIFRUR_CDS_LE_ISKA: "string"       // CDS transaction code
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 88212,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAK160 - Local Phone
     * Local (Israeli) phone numbers.
     * SUG_MIVNE_KTOVET=60 in ATAKL10.
     */
    ATAK160: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        SUG_KTOVET_TELEPHON: "string",     // Phone type code
        KIDOMET_TELEPHON: "string",        // Phone prefix (area code)
        EZOR_CHIUG: "string",              // Dialing area
        MISPAR_TELEPHON: "string",         // Phone number
        KOD_TELEPHON_O_FAX: "number",      // Phone or fax indicator (2,3=fax)
        MISPAR_IBAN: "string",             // IBAN number
        SUG_TELEPHON: "string",            // Phone type
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string",        // Source table name
        SIFRUR_CDS_LE_ISKA: "string"       // CDS transaction code
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 197365,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAK166 - Abroad Phone
     * International phone numbers.
     * SUG_MIVNE_KTOVET=66 in ATAKL10.
     */
    ATAK166: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        KOD_ERETZ: "number",               // Country code
        KIDOMET_ERETZ_TEL: "string",       // Country dialing prefix
        KIDOMET_YISHUV: "string",          // City area prefix
        MPR_TELEPHON_CHUL: "string",       // Foreign phone number
        KOD_TELEPHON_O_FAX: "number",      // Phone or fax indicator
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string",        // Source table name
        SIFRUR_CDS_LE_ISKA: "string"       // CDS transaction code
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 88595,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    },

    /**
     * ATAK190 - Email Address
     * Email contact information.
     * SUG_MIVNE_KTOVET=90 in ATAKL10.
     */
    ATAK190: {
      fields: {
        _id: "ObjectId",
        MISPAR_KTOVET: "number",           // Address serial ID - unique key
        KTOVET_E_MAIL: "string",           // Email address
        KOD_MARC_MAKOR: "number",          // Source system code
        TIMESTAMP_SHINUA_ROW: "date",      // Row change timestamp
        LOAD_DATE: "date",                 // ETL load date
        BRAIN_TABLE_NAME: "string",        // Source table name
        SIFRUR_CDS_LE_ISKA: "string"       // CDS transaction code
      },
      indexes: [
        { name: "_id_", keys: { _id: 1 }, unique: false },
        { name: "MISPAR_KTOVET_1", keys: { MISPAR_KTOVET: 1 }, unique: false }
      ],
      approximateDocCount: 127958,
      uniqueKey: {
        fields: ["MISPAR_KTOVET"],
        source: "inferred",
        confidence: 95
      }
    }
  },

  relationships: [
    // ATAKL10 is the central hub linking to all detail tables
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAK112",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "Local address details (SUG_MIVNE_KTOVET=12)",
      indexed: { from: false, to: true }
    },
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAKL12",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "Local address extension fields",
      indexed: { from: false, to: true }
    },
    {
      from: "ATAK112",
      fromField: "MISPAR_KTOVET",
      to: "ATAKL12",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "ATAK112 and ATAKL12 are companion tables for local addresses",
      indexed: { from: true, to: true }
    },
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAK116",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "Abroad address details (SUG_MIVNE_KTOVET=16)",
      indexed: { from: false, to: true }
    },
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAK120",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "P.O. Box address (SUG_MIVNE_KTOVET=20)",
      indexed: { from: false, to: true }
    },
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAK140",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "P.O. Box in branch (SUG_MIVNE_KTOVET=40)",
      indexed: { from: false, to: true }
    },
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAK150",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "SWIFT address (SUG_MIVNE_KTOVET=50)",
      indexed: { from: false, to: true }
    },
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAK160",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "Local phone (SUG_MIVNE_KTOVET=60)",
      indexed: { from: false, to: true }
    },
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAK166",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "Abroad phone (SUG_MIVNE_KTOVET=66)",
      indexed: { from: false, to: true }
    },
    {
      from: "ATAKL10",
      fromField: "MISPAR_KTOVET",
      to: "ATAK190",
      toField: "MISPAR_KTOVET",
      cardinality: "1:1",
      description: "Email address (SUG_MIVNE_KTOVET=90)",
      indexed: { from: false, to: true }
    }
  ],

  notes: [
    // Optimization hints for pipeline design
    "ATAKL10 is the central routing table - always start queries here for customer contact lookups",
    "ATAKL10.SIFRUR_LAKOACH + SUG_MIVNE_KTOVET has a compound index - use for customer-by-address-type queries",
    "All detail tables (ATAK1xx) have MISPAR_KTOVET indexed - joins are efficient in both directions",
    "ATAKL10.MISPAR_KTOVET is NOT indexed - lookups from detail to header require collection scan or $lookup with index on 'to' side",
    "Consider adding index on ATAKL10.MISPAR_KTOVET if reverse lookups are frequent",
    "ATAK112 and ATAKL12 have identical doc counts (600,447) - they form a 1:1 pair for local addresses",
    "SUG_MIVNE_KTOVET values map to tables: 12→ATAK112/ATAKL12, 16→ATAK116, 20→ATAK120, 40→ATAK140, 50→ATAK150, 60→ATAK160, 66→ATAK166, 90→ATAK190",
    "For PartyContact synthesis: group ATAKL10 by SIFRUR_LAKOACH, then $lookup each ATAK1xx by MISPAR_KTOVET filtered by SUG_MIVNE_KTOVET",
    "All unique keys are inferred (no unique indexes) - consider adding unique index on MISPAR_KTOVET if write conflicts are a concern",
    "1:N fan-out: One customer (SIFRUR_LAKOACH) can have multiple addresses - aggregate carefully to avoid memory issues"
  ]
};

module.exports = schema;

module.exports = [
  // Start from ATAKL10 - the main address-party relationship table
  // Group by SIFRUR_LAKOACH to create one document per party with all their contacts
  {
    $group: {
      _id: "$SIFRUR_LAKOACH",
      addresses: { $push: "$$ROOT" }
    }
  },
  
  // Lookup local address details (SUG_MIVNE_KTOVET=12) from ATAK112
  {
    $lookup: {
      from: "ATAK112",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 12] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "localAddressDetails"
    }
  },
  
  // Lookup ATAKL12 for additional local address fields
  {
    $lookup: {
      from: "ATAKL12",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 12] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "localAddressExtras"
    }
  },
  
  // Lookup abroad address details (SUG_MIVNE_KTOVET=16) from ATAK116
  {
    $lookup: {
      from: "ATAK116",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 16] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "abroadAddressDetails"
    }
  },
  
  // Lookup POB details (SUG_MIVNE_KTOVET=20) from ATAK120
  {
    $lookup: {
      from: "ATAK120",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 20] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "pobDetails"
    }
  },
  
  // Lookup branch POB details (SUG_MIVNE_KTOVET=40) from ATAK140
  {
    $lookup: {
      from: "ATAK140",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 40] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "pobBranchDetails"
    }
  },
  
  // Lookup swift address details (SUG_MIVNE_KTOVET=50) from ATAK150
  {
    $lookup: {
      from: "ATAK150",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 50] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "swiftDetails"
    }
  },
  
  // Lookup local phone details (SUG_MIVNE_KTOVET=60) from ATAK160
  {
    $lookup: {
      from: "ATAK160",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 60] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "localPhoneDetails"
    }
  },
  
  // Lookup abroad phone details (SUG_MIVNE_KTOVET=66) from ATAK166
  {
    $lookup: {
      from: "ATAK166",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 66] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "abroadPhoneDetails"
    }
  },
  
  // Lookup email details (SUG_MIVNE_KTOVET=90) from ATAK190
  {
    $lookup: {
      from: "ATAK190",
      let: { addrs: "$addresses" },
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ["$MISPAR_KTOVET", {
                $map: {
                  input: { $filter: { input: "$$addrs", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 90] } } },
                  as: "a",
                  in: "$$a.MISPAR_KTOVET"
                }
              }]
            }
          }
        }
      ],
      as: "emailDetails"
    }
  },
  
  // Project into the target PartyContact structure
  {
    $project: {
      _id: 1,
      contact: {
        // Local Address (SUG_MIVNE_KTOVET=12)
        localAddress: {
          $map: {
            input: { $filter: { input: "$addresses", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 12] } } },
            as: "addr",
            in: {
              $let: {
                vars: {
                  detail: {
                    $arrayElemAt: [
                      { $filter: { input: "$localAddressDetails", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  },
                  extra: {
                    $arrayElemAt: [
                      { $filter: { input: "$localAddressExtras", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  }
                },
                in: {
                  addressSerialId: "$$addr.MISPAR_KTOVET",
                  city: "$$detail.SHEM_YISHUV_TIKNI",
                  cityCode: "$$detail.SEMEL_YISHUV",
                  street: "$$detail.SHEM_RHV_MINHALI",
                  streetCode: "$$detail.SEMEL_RECHOV",
                  buildingNumber: "$$detail.MPR_BAYIT_MINHALI",
                  letterInHouseNumber: "$$extra.OT_BE_MISPAR_BAYIT",
                  entranceNumber: {
                    $ifNull: ["$$extra.MISPAR_KNISA", "$$detail.MISPAR_KNISA"]
                  },
                  apartmentNumber: "$$extra.MPR_DIRA_MINHALI",
                  zipCode: "$$detail.MIKUD_CHADASH",
                  zipCode5: "$$detail.MIKUD",
                  nameOnMailbox: "$$extra.SHEM_BE_TEVAT_DOAR",
                  notes: "$$extra.MEAFYEN_NOSAF",
                  workingPlace: "$$extra.SHEM_MAKOM_AVODA",
                  contactAttributes: {
                    faultAddressCode: {
                      $cond: {
                        if: { $eq: ["$$addr.KOD_KTOVET_MSB", 1] },
                        then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                        else: "$$REMOVE"
                      }
                    },
                    contactPriority: {
                      $cond: {
                        if: { $in: ["$$addr.SUG_KTOVET", [1, 2]] },
                        then: "$$addr.RAMAT_ADIFUT",
                        else: "$$REMOVE"
                      }
                    },
                    partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                    partyAddressUsages: [{
                      partyAddressUsageCode: "$$addr.SUG_KTOVET",
                      isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                      invalidAddressCode: {
                        $cond: {
                          if: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                          then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                          else: "$$REMOVE"
                        }
                      }
                    }],
                    updatingDate: "$$addr.TAARICH10_IDKUN",
                    imSource: "$$addr.MAKOR_BE_IMS"
                  }
                }
              }
            }
          }
        },
        
        // Abroad Address (SUG_MIVNE_KTOVET=16)
        abroadAddress: {
          $map: {
            input: { $filter: { input: "$addresses", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 16] } } },
            as: "addr",
            in: {
              $let: {
                vars: {
                  detail: {
                    $arrayElemAt: [
                      { $filter: { input: "$abroadAddressDetails", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  }
                },
                in: {
                  addressSerialId: "$$addr.MISPAR_KTOVET",
                  countryCode: { code: "$$detail.KOD_ERETZ" },
                  state: "$$detail.SHEM_MEDINA",
                  city: "$$detail.SHEM_YISHUV_CHUL",
                  street: "$$detail.SHEM_RHV_MNHL_LAZ",
                  buildingNumber: "$$detail.MPR_BAYIT_MINHALI",
                  zipCode: "$$detail.MIKUD_CHUL",
                  contactAttributes: {
                    faultAddressCode: {
                      $cond: {
                        if: { $eq: ["$$addr.KOD_KTOVET_MSB", 1] },
                        then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                        else: "$$REMOVE"
                      }
                    },
                    contactPriority: {
                      $cond: {
                        if: { $in: ["$$addr.SUG_KTOVET", [1, 2]] },
                        then: "$$addr.RAMAT_ADIFUT",
                        else: "$$REMOVE"
                      }
                    },
                    partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                    partyAddressUsages: [{
                      partyAddressUsageCode: "$$addr.SUG_KTOVET",
                      isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                      invalidAddressCode: {
                        $cond: {
                          if: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                          then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                          else: "$$REMOVE"
                        }
                      }
                    }],
                    updatingDate: "$$addr.TAARICH10_IDKUN",
                    imSource: "$$addr.MAKOR_BE_IMS"
                  }
                }
              }
            }
          }
        },
        
        // POB (SUG_MIVNE_KTOVET=20)
        pOB: {
          $map: {
            input: { $filter: { input: "$addresses", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 20] } } },
            as: "addr",
            in: {
              $let: {
                vars: {
                  detail: {
                    $arrayElemAt: [
                      { $filter: { input: "$pobDetails", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  }
                },
                in: {
                  addressSerialId: "$$addr.MISPAR_KTOVET",
                  pOBNumber: "$$detail.TA_DOAR",
                  pOBTypeCode: "$$detail.SUG_TA_DOAR",
                  city: "$$detail.SHEM_YISHUV_TIKNI",
                  cityCode: "$$detail.SEMEL_YISHUV",
                  zipCode: "$$detail.MIKUD_CHADASH",
                  zipCode5: "$$detail.MIKUD",
                  contactAttributes: {
                    faultAddressCode: {
                      $cond: {
                        if: { $eq: ["$$addr.KOD_KTOVET_MSB", 1] },
                        then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                        else: "$$REMOVE"
                      }
                    },
                    contactPriority: {
                      $cond: {
                        if: { $in: ["$$addr.SUG_KTOVET", [1, 2]] },
                        then: "$$addr.RAMAT_ADIFUT",
                        else: "$$REMOVE"
                      }
                    },
                    partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                    partyAddressUsages: [{
                      partyAddressUsageCode: "$$addr.SUG_KTOVET",
                      isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                      invalidAddressCode: {
                        $cond: {
                          if: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                          then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                          else: "$$REMOVE"
                        }
                      }
                    }],
                    updatingDate: "$$addr.TAARICH10_IDKUN",
                    imSource: "$$addr.MAKOR_BE_IMS"
                  }
                }
              }
            }
          }
        },
        
        // POB in Branch (SUG_MIVNE_KTOVET=40)
        pOBInBranch: {
          $map: {
            input: { $filter: { input: "$addresses", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 40] } } },
            as: "addr",
            in: {
              $let: {
                vars: {
                  detail: {
                    $arrayElemAt: [
                      { $filter: { input: "$pobBranchDetails", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  }
                },
                in: {
                  addressSerialId: "$$addr.MISPAR_KTOVET",
                  branchNumber: "$$detail.MISPAR_SNIF",
                  pOBNumber: "$$detail.MPR_TA_DOAR_BASNIF",
                  contactAttributes: {
                    faultAddressCode: {
                      $cond: {
                        if: { $eq: ["$$addr.KOD_KTOVET_MSB", 1] },
                        then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                        else: "$$REMOVE"
                      }
                    },
                    contactPriority: {
                      $cond: {
                        if: { $in: ["$$addr.SUG_KTOVET", [1, 2]] },
                        then: "$$addr.RAMAT_ADIFUT",
                        else: "$$REMOVE"
                      }
                    },
                    partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                    partyAddressUsages: [{
                      partyAddressUsageCode: "$$addr.SUG_KTOVET",
                      isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                      invalidAddressCode: {
                        $cond: {
                          if: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                          then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                          else: "$$REMOVE"
                        }
                      }
                    }],
                    updatingDate: "$$addr.TAARICH10_IDKUN",
                    imSource: "$$addr.MAKOR_BE_IMS"
                  }
                }
              }
            }
          }
        },
        
        // Swift Address (SUG_MIVNE_KTOVET=50)
        swiftAddress: {
          $map: {
            input: { $filter: { input: "$addresses", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 50] } } },
            as: "addr",
            in: {
              $let: {
                vars: {
                  detail: {
                    $arrayElemAt: [
                      { $filter: { input: "$swiftDetails", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  }
                },
                in: {
                  addressSerialId: "$$addr.MISPAR_KTOVET",
                  swiftBank: "$$detail.KOD_BANK_SWIFT",
                  swiftCountryCode: "$$detail.KOD_MEDINA_SWIFT",
                  swiftCityCode: "$$detail.KOD_IR_SWIFT",
                  swiftAdditionalUnit: "$$detail.MPRSNT_O_YCD_O_NSF",
                  contactAttributes: {
                    faultAddressCode: {
                      $cond: {
                        if: { $eq: ["$$addr.KOD_KTOVET_MSB", 1] },
                        then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                        else: "$$REMOVE"
                      }
                    },
                    contactPriority: {
                      $cond: {
                        if: { $in: ["$$addr.SUG_KTOVET", [1, 2]] },
                        then: "$$addr.RAMAT_ADIFUT",
                        else: "$$REMOVE"
                      }
                    },
                    partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                    partyAddressUsages: [{
                      partyAddressUsageCode: "$$addr.SUG_KTOVET",
                      isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                      invalidAddressCode: {
                        $cond: {
                          if: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                          then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                          else: "$$REMOVE"
                        }
                      }
                    }],
                    updatingDate: "$$addr.TAARICH10_IDKUN",
                    imSource: "$$addr.MAKOR_BE_IMS"
                  }
                }
              }
            }
          }
        },
        
        // Local Phone (SUG_MIVNE_KTOVET=60)
        localPhone: {
          $map: {
            input: { $filter: { input: "$addresses", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 60] } } },
            as: "addr",
            in: {
              $let: {
                vars: {
                  detail: {
                    $arrayElemAt: [
                      { $filter: { input: "$localPhoneDetails", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  }
                },
                in: {
                  addressSerialId: "$$addr.MISPAR_KTOVET",
                  phoneType: { code: "$$detail.SUG_TELEPHON" },
                  phonePrefix: "$$detail.EZOR_CHIUG",
                  phoneNumber: "$$detail.MISPAR_TELEPHON",
                  isFax: { $in: ["$$detail.KOD_TELEPHON_O_FAX", [2, 3]] },
                  contactAttributes: {
                    faultAddressCode: {
                      $cond: {
                        if: { $eq: ["$$addr.KOD_KTOVET_MSB", 1] },
                        then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                        else: "$$REMOVE"
                      }
                    },
                    contactPriority: {
                      $cond: {
                        if: { $in: ["$$addr.SUG_KTOVET", [1, 2]] },
                        then: "$$addr.RAMAT_ADIFUT",
                        else: "$$REMOVE"
                      }
                    },
                    partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                    partyAddressUsages: [{
                      partyAddressUsageCode: "$$addr.SUG_KTOVET",
                      isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                      invalidAddressCode: {
                        $cond: {
                          if: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                          then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                          else: "$$REMOVE"
                        }
                      }
                    }],
                    updatingDate: "$$addr.TAARICH10_IDKUN",
                    imSource: "$$addr.MAKOR_BE_IMS"
                  },
                  updatingDate: "$$addr.TAARICH10_IDKUN",
                  imSource: "$$addr.MAKOR_BE_IMS"
                }
              }
            }
          }
        },
        
        // Abroad Phone (SUG_MIVNE_KTOVET=66)
        abroadPhone: {
          $map: {
            input: { $filter: { input: "$addresses", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 66] } } },
            as: "addr",
            in: {
              $let: {
                vars: {
                  detail: {
                    $arrayElemAt: [
                      { $filter: { input: "$abroadPhoneDetails", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  }
                },
                in: {
                  addressSerialId: "$$addr.MISPAR_KTOVET",
                  abroadPhoneCountryCode: "$$detail.KIDOMET_ERETZ_TEL",
                  abroadPhoneAreaPrefix: "$$detail.KIDOMET_YISHUV",
                  phoneNumber: "$$detail.MPR_TELEPHON_CHUL",
                  isFax: { $eq: ["$$detail.KOD_TELEPHON_O_FAX", 2] },
                  contactAttributes: {
                    faultAddressCode: {
                      $cond: {
                        if: { $eq: ["$$addr.KOD_KTOVET_MSB", 1] },
                        then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                        else: "$$REMOVE"
                      }
                    },
                    contactPriority: {
                      $cond: {
                        if: { $in: ["$$addr.SUG_KTOVET", [1, 2]] },
                        then: "$$addr.RAMAT_ADIFUT",
                        else: "$$REMOVE"
                      }
                    },
                    partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                    partyAddressUsages: [{
                      partyAddressUsageCode: "$$addr.SUG_KTOVET",
                      isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                      invalidAddressCode: {
                        $cond: {
                          if: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                          then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                          else: "$$REMOVE"
                        }
                      }
                    }],
                    updatingDate: "$$addr.TAARICH10_IDKUN",
                    imSource: "$$addr.MAKOR_BE_IMS"
                  }
                }
              }
            }
          }
        },
        
        // Email (SUG_MIVNE_KTOVET=90)
        email: {
          $map: {
            input: { $filter: { input: "$addresses", cond: { $eq: ["$$this.SUG_MIVNE_KTOVET", 90] } } },
            as: "addr",
            in: {
              $let: {
                vars: {
                  detail: {
                    $arrayElemAt: [
                      { $filter: { input: "$emailDetails", cond: { $eq: ["$$this.MISPAR_KTOVET", "$$addr.MISPAR_KTOVET"] } } },
                      0
                    ]
                  }
                },
                in: {
                  addressSerialId: "$$addr.MISPAR_KTOVET",
                  emailAddress: "$$detail.KTOVET_E_MAIL",
                  contactAttributes: {
                    faultAddressCode: {
                      $cond: {
                        if: { $eq: ["$$addr.KOD_KTOVET_MSB", 1] },
                        then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                        else: "$$REMOVE"
                      }
                    },
                    contactPriority: {
                      $cond: {
                        if: { $in: ["$$addr.SUG_KTOVET", [1, 2]] },
                        then: "$$addr.RAMAT_ADIFUT",
                        else: "$$REMOVE"
                      }
                    },
                    partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                    partyAddressUsages: [{
                      partyAddressUsageCode: "$$addr.SUG_KTOVET",
                      isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                      invalidAddressCode: {
                        $cond: {
                          if: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] },
                          then: { code: "$$addr.KOD_SIBAT_SBS_KTV" },
                          else: "$$REMOVE"
                        }
                      }
                    }],
                    updatingDate: "$$addr.TAARICH10_IDKUN",
                    imSource: "$$addr.MAKOR_BE_IMS"
                  }
                }
              }
            }
          }
        }
      }
    }
  }
];

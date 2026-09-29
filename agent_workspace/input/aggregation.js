// Start from ATAKL10, group by party and build all contact arrays
module.exports = [
  // ATAKL10 has the address routing with SUG_MIVNE_KTOVET
  // We group by SIFRUR_LAKOACH (party ID) to collect all addresses per party
  {
    $group: {
      _id: "$SIFRUR_LAKOACH",
      addresses: { $push: "$$ROOT" }
    }
  },
  
  // Project the final structure
  {
    $project: {
      _id: 1,
      contact: {
        // Local Address array (SUG_MIVNE_KTOVET = 12)
        localAddress: {
          $map: {
            input: { $filter: { input: "$addresses", as: "a", cond: { $eq: ["$$a.SUG_MIVNE_KTOVET", 12] } } },
            as: "addr",
            in: {
              // Handle BSON Extended JSON $numberLong format
              addressSerialId: {
                $cond: {
                  if: { $ne: [{ $type: "$$addr.MISPAR_KTOVET" }, "object"] },
                  then: "$$addr.MISPAR_KTOVET",
                  else: { $toLong: "$$addr.MISPAR_KTOVET.$numberLong" }
                }
              },
              contactAttributes: {
                partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                partyAddressUsages: [{
                  partyAddressUsageCode: "$$addr.SUG_KTOVET",
                  isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] }
                }]
              }
            }
          }
        },
        
        // Abroad Address array (SUG_MIVNE_KTOVET = 16)
        abroadAddress: {
          $map: {
            input: { $filter: { input: "$addresses", as: "a", cond: { $eq: ["$$a.SUG_MIVNE_KTOVET", 16] } } },
            as: "addr",
            in: {
              addressSerialId: {
                $cond: {
                  if: { $ne: [{ $type: "$$addr.MISPAR_KTOVET" }, "object"] },
                  then: "$$addr.MISPAR_KTOVET",
                  else: { $toLong: "$$addr.MISPAR_KTOVET.$numberLong" }
                }
              },
              contactAttributes: {
                partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                partyAddressUsages: [{
                  partyAddressUsageCode: "$$addr.SUG_KTOVET",
                  isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] }
                }]
              }
            }
          }
        },
        
        // POB array (SUG_MIVNE_KTOVET = 20)
        pOB: {
          $map: {
            input: { $filter: { input: "$addresses", as: "a", cond: { $eq: ["$$a.SUG_MIVNE_KTOVET", 20] } } },
            as: "addr",
            in: {
              addressSerialId: {
                $cond: {
                  if: { $ne: [{ $type: "$$addr.MISPAR_KTOVET" }, "object"] },
                  then: "$$addr.MISPAR_KTOVET",
                  else: { $toLong: "$$addr.MISPAR_KTOVET.$numberLong" }
                }
              },
              contactAttributes: {
                partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                partyAddressUsages: [{
                  partyAddressUsageCode: "$$addr.SUG_KTOVET",
                  isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] }
                }]
              }
            }
          }
        },
        
        // POB in Branch array (SUG_MIVNE_KTOVET = 40)
        pOBInBranch: {
          $map: {
            input: { $filter: { input: "$addresses", as: "a", cond: { $eq: ["$$a.SUG_MIVNE_KTOVET", 40] } } },
            as: "addr",
            in: {
              addressSerialId: {
                $cond: {
                  if: { $ne: [{ $type: "$$addr.MISPAR_KTOVET" }, "object"] },
                  then: "$$addr.MISPAR_KTOVET",
                  else: { $toLong: "$$addr.MISPAR_KTOVET.$numberLong" }
                }
              },
              contactAttributes: {
                partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                partyAddressUsages: [{
                  partyAddressUsageCode: "$$addr.SUG_KTOVET",
                  isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] }
                }]
              }
            }
          }
        },
        
        // Swift Address array (SUG_MIVNE_KTOVET = 50)
        swiftAddress: {
          $map: {
            input: { $filter: { input: "$addresses", as: "a", cond: { $eq: ["$$a.SUG_MIVNE_KTOVET", 50] } } },
            as: "addr",
            in: {
              addressSerialId: {
                $cond: {
                  if: { $ne: [{ $type: "$$addr.MISPAR_KTOVET" }, "object"] },
                  then: "$$addr.MISPAR_KTOVET",
                  else: { $toLong: "$$addr.MISPAR_KTOVET.$numberLong" }
                }
              },
              contactAttributes: {
                partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                partyAddressUsages: [{
                  partyAddressUsageCode: "$$addr.SUG_KTOVET",
                  isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] }
                }]
              }
            }
          }
        },
        
        // Local Phone array (SUG_MIVNE_KTOVET = 60)
        localPhone: {
          $map: {
            input: { $filter: { input: "$addresses", as: "a", cond: { $eq: ["$$a.SUG_MIVNE_KTOVET", 60] } } },
            as: "addr",
            in: {
              addressSerialId: {
                $cond: {
                  if: { $ne: [{ $type: "$$addr.MISPAR_KTOVET" }, "object"] },
                  then: "$$addr.MISPAR_KTOVET",
                  else: { $toLong: "$$addr.MISPAR_KTOVET.$numberLong" }
                }
              },
              contactAttributes: {
                partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                partyAddressUsages: [{
                  partyAddressUsageCode: "$$addr.SUG_KTOVET",
                  isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] }
                }]
              }
            }
          }
        },
        
        // Abroad Phone array (SUG_MIVNE_KTOVET = 66)
        abroadPhone: {
          $map: {
            input: { $filter: { input: "$addresses", as: "a", cond: { $eq: ["$$a.SUG_MIVNE_KTOVET", 66] } } },
            as: "addr",
            in: {
              addressSerialId: {
                $cond: {
                  if: { $ne: [{ $type: "$$addr.MISPAR_KTOVET" }, "object"] },
                  then: "$$addr.MISPAR_KTOVET",
                  else: { $toLong: "$$addr.MISPAR_KTOVET.$numberLong" }
                }
              },
              contactAttributes: {
                partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                partyAddressUsages: [{
                  partyAddressUsageCode: "$$addr.SUG_KTOVET",
                  isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] }
                }]
              }
            }
          }
        },
        
        // Email array (SUG_MIVNE_KTOVET = 90)
        email: {
          $map: {
            input: { $filter: { input: "$addresses", as: "a", cond: { $eq: ["$$a.SUG_MIVNE_KTOVET", 90] } } },
            as: "addr",
            in: {
              addressSerialId: {
                $cond: {
                  if: { $ne: [{ $type: "$$addr.MISPAR_KTOVET" }, "object"] },
                  then: "$$addr.MISPAR_KTOVET",
                  else: { $toLong: "$$addr.MISPAR_KTOVET.$numberLong" }
                }
              },
              contactAttributes: {
                partyAddressUsage: [{ code: "$$addr.SUG_KTOVET" }],
                partyAddressUsages: [{
                  partyAddressUsageCode: "$$addr.SUG_KTOVET",
                  isInvalidAddress: { $ne: ["$$addr.KOD_KTOVET_MSB", 0] }
                }]
              }
            }
          }
        }
      }
    }
  }
];

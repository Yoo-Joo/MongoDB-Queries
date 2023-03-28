// Group each service by it's own type
db['services-type'].aggregate([
    { $match: { service: { $in : ['1.A Professional Services', '82 Legal and accounting services', '861 Legal services', '2.A Postal services'] } } },
    { $group: { _id: '$type', services: { $push: '$service' } } },
    { $project: { _id: 0, type: '$_id', services: 1 } }
])

// Group number of offers by countries filtered by all services (new one)
db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
    { $sort: { versionNumber: -1 } },
    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' } } },
    { $unwind: '$sectors' },
    { $project: { countryNameCreator: 1, service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
    { $group: { _id: '$service', countries: { $push: '$_id' }, cpc_code: { $push: '$cpc_code' }, provisional_code: { $push: '$provisional_code' } } }
//    { $match: { $or: [ { service: { $in: ['1.A Professional Services', '2.A Postal services'] } }, { cpc_code: { $in: ['833 Engineering services', '6801 Postal services'] } }, { provisional_code: { $in: ['861 Legal services', '9402 Refuse disposal services'] } } ] } },
//    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
//    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
//    { $group: { _id: { _id: '$_id', service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' } } },
//    { $project: { _id: '$_id._id', service: '$_id.service', cpc_code: '$_id.cpc_code', provisional_code: '$_id.provisional_code' } },
//    { $group: { _id: { service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' }, countries: { $push: '$_id' }, nb_offers: { '$sum': 1 } } },
//    { $project: { _id: 0, services: '$_id', countries: 1, nb_offers: 1 } }
])


// Group number of offers by countries filtered by all services
db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
    { $sort: { versionNumber: -1 } },
    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' } } },
    { $unwind: '$sectors' },
    { $project: { countryNameCreator: 1, service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
    { $match: { $or: [ { service: { $in: [] } }, { cpc_code: { $in: [] } }, { provisional_code: { $in: ['861 Legal services'] } } ] } },
    { $match: { _id: { $in: ['Madagascar', 'Nigeria', 'Sudan', 'Togo'] } } },
    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
    { $match: { $or: [ { service : { $in: [] } }, { cpc_code: { $in: [] } }, { provisional_code: { $in: ['861 Legal services'] } } ] } },
//    { $group: { _id: { _id: '$_id', service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' } } },
//    { $project: { _id: '$_id._id', service: '$_id.service', cpc_code: '$_id.cpc_code', provisional_code: '$_id.provisional_code' } },
//    { $group: { _id: { service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' }, countries: { $push: '$_id' }, nb_offers: { '$sum': 1 } } },
//    { $project: { _id: 0, services: '$_id', countries: 1, nb_offers: 1 } },
//    { $addFields: { service: '$services.service', cpc_code: '$services.cpc_code', provisional_code: '$services.provisional_code' } },
//    { $project: { services: 0 } }, this one
//    { $group: { _id: '$service', nb_offers: { '$sum': '$nb_offers' } , cpc_code: { $push: { $cond: { if: { $gt: [{$type: '$cpc_code'}, 'missing'] }, then: { service: '$cpc_code', countries: '$countries', nb_offers: '$nb_offers' }, else: '$$REMOVE' } } }, provisional_code: { $push: { $cond: { if: { $gt: [{$type: '$provisional_code'}, 'missing'] }, then: { service: '$provisional_code', countries: '$countries', nb_offers: '$nb_offers' }, else: '$$REMOVE' } } } } },
//    { $project: { _id: 0, service: 0, service: '$_id', nb_offers: 1, cpc_code: 1, provisional_code: 1 } }
//    { $group: { _id: '$service', sub_services: { $push: { cpc_code: '$cpc_code', provisional_code: '$provisional_code', countries: '$countries', nb_offers: '$nb_offers' } } } },this one
//    { $project: { _id: 0, service: '$_id', sub_services: 1 } } this one
])


// Group offers by countries
db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT' } },
    { $sort: { versionNumber: -1 } },
    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' } } },
    { $unwind: '$sectors' },
    { $project: { countryNameCreator: 1, service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
    { $match: { $or: [ { service: { $in: ['1.A Professional Services', '2.A Postal services'] } }, { cpc_code: { $in: ['833 Engineering services', '6801 Postal services'] } }, { provisional_code: { $in: ['861 Legal services', '9402 Refuse disposal services'] } } ] } },
    { $match: { _id: { $in: ['Madagascar', 'Nigeria', 'Sudan', 'Togo'] } } },
    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
    { $group: { _id: { _id: '$_id', service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' } } },
    { $project: { _id: '$_id._id', service: '$_id.service', cpc_code: '$_id.cpc_code', provisional_code: '$_id.provisional_code' } },
    { $group: { _id: { service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' }, countries: { $push: '$_id' }, nb_offers: { '$sum': 1 } } },
    { $project: { _id: 0, services: '$_id', countries: 1, nb_offers: 1 } }
])


// Another method
db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
    { $sort: { versionNumber: -1 } },
    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' } } },
    { $unwind: '$sectors' },
    { $project: { countryNameCreator: 1, service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
    { $match: { $or: [ { service: { $in: ['1.A Professional Services', '2.A Postal services', '833 Engineering services', '6801 Postal services', '861 Legal services', '9402 Refuse disposal services'] } }, { cpc_code: { $in: ['1.A Professional Services', '2.A Postal services', '833 Engineering services', '6801 Postal services', '861 Legal services', '9402 Refuse disposal services'] } }, { provisional_code: { $in: ['1.A Professional Services', '2.A Postal services', '833 Engineering services', '6801 Postal services', '861 Legal services', '9402 Refuse disposal services'] } } ] } },
    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
    { $group: { _id: { _id: '$_id', service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' } } },
    { $project: { _id: '$_id._id', service: '$_id.service', cpc_code: '$_id.cpc_code', provisional_code: '$_id.provisional_code' } },
    { $group: { _id: { service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' }, countries: { $push: '$_id' }, nb_offers: { '$sum': 1 } } },
    { $project: { _id: 0, services: '$_id', countries: 1, nb_offers: 1 } }
])


// Group number of offers by limitations on national treatment filtered by all services
db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
    { $sort: { versionNumber: -1 } },
    { $group: { _id: '$countryNameCreator', sectors : { $first: '$sectors' } } },
    { $unwind: '$sectors' },
    { $project: { commitement: '$sectors.sectorCommitement', service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
    { $match: { $or: [ { service: { $in: ['1.A Professional Services', '2.A Postal services'] } }, { cpc_code: { $in: ['833 Engineering services', '6801 Postal services'] } }, { provisional_code: { $in: ['861 Legal services', '9402 Refuse disposal services'] } } ] } },
    { $match: { _id: { $in: ['Madagascar', 'Nigeria', 'Sudan', 'Togo'] } } },
    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
    { $project: { service: 1, cpc_code: 1, provisional_code: 1, modes: [ '$commitement.limitationsOnNationalTreatment.crossBorder.value', '$commitement.limitationsOnNationalTreatment.consumptionAbroad.value', '$commitement.limitationsOnNationalTreatment.commercialPresence.value', '$commitement.limitationsOnNationalTreatment.presenceOfNaturalPersons.value' ] } },
    { $unwind: '$modes' },
    { $group: { _id: { service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code', country: '$_id' }, Other: { $sum: { $cond: [ { $eq: ['$modes', 'Other'] }, 1, 0 ] } }, None: { $sum: { $cond: [ { $eq: ['$modes', 'None'] }, 1, 0 ] } }, Unbound: { $sum: { $cond: [ { $eq: ['$modes', 'Unbound'] }, 1, 0 ] } }, 'Unbound*': { $sum: { $cond: [ { $eq: ['$modes', 'Unbound*'] }, 1, 0 ] } } } },
    { $group: { _id: { service: '$_id.service', cpc_code: '$_id.cpc_code', provisional_code: '$_id.provisional_code' }, OtherByCountry: { '$push': { $cond: [ { $gt: ['$Other', 0] }, { country: '$_id.country', count: '$Other' }, null ] } }, NoneByCountry: { '$push': { $cond: [ { $gt: ['$None', 0] }, { country: '$_id.country', count: '$None' }, null ] } }, UnboundByCountry: { '$push': { $cond: [ { $gt: ['$Unbound', 0] }, { country: '$_id.country', count: '$Unbound' }, null ] } }, 'Unbound*ByCountry': { '$push': { $cond: [ { $gt: ['$Unbound*', 0] }, { country: '$_id.country', count: '$Unbound*' }, null ] } }, Other: { $sum: '$Other' }, None: { $sum: '$None' }, Unbound: { $sum: '$Unbound' }, 'Unbound*': { $sum: '$Unbound*' } } },
    { $project: { _id: 0, services: '$_id', Other: 1, None: 1, Unbound: 1, 'Unbound*': 1, OtherByCountry: { $setDifference: ['$OtherByCountry', [null]] }, NoneByCountry: { $setDifference: ['$NoneByCountry', [null]] }, UnboundByCountry: { $setDifference: ['$UnboundByCountry', [null]] }, 'Unbound*ByCountry': { $setDifference: ['$Unbound*ByCountry', [null]] } } }
])


// Group number of offers by limitations on MA filtered by all services
db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
    { $sort: { versionNumber: -1 } },
    { $group: { _id: '$countryNameCreator', sectors : { $first: '$sectors' } } },
    { $unwind: '$sectors' },
    { $project: { commitement: '$sectors.sectorCommitement', service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
    { $match: { $or: [ { service: { $in: ['1.A Professional Services', '2.A Postal services'] } }, { cpc_code: { $in: ['833 Engineering services', '6801 Postal services'] } }, { provisional_code: { $in: ['861 Legal services', '9402 Refuse disposal services'] } } ] } },
    { $match: { _id: { $in: ['Madagascar', 'Nigeria', 'Sudan', 'Togo'] } } },
    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
    { $project: { service: 1, cpc_code: 1, provisional_code: 1, modes: [ '$commitement.limitationsMA.crossBorder.value', '$commitement.limitationsMA.consumptionAbroad.value', '$commitement.limitationsMA.commercialPresence.value', '$commitement.limitationsMA.presenceOfNaturalPersons.value' ] } },
    { $unwind: '$modes' },
    { $group: { _id: { service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code', country: '$_id' }, Other: { $sum: { $cond: [ { $eq: ['$modes', 'Other'] }, 1, 0 ] } }, None: { $sum: { $cond: [ { $eq: ['$modes', 'None'] }, 1, 0 ] } }, Unbound: { $sum: { $cond: [ { $eq: ['$modes', 'Unbound'] }, 1, 0 ] } }, 'Unbound*': { $sum: { $cond: [ { $eq: ['$modes', 'Unbound*'] }, 1, 0 ] } } } },
    { $group: { _id: { service: '$_id.service', cpc_code: '$_id.cpc_code', provisional_code: '$_id.provisional_code' }, OtherByCountry: { '$push': { $cond: [ { $gt: ['$Other', 0] }, { country: '$_id.country', count: '$Other' }, null ] } }, NoneByCountry: { '$push': { $cond: [ { $gt: ['$None', 0] }, { country: '$_id.country', count: '$None' }, null ] } }, UnboundByCountry: { '$push': { $cond: [ { $gt: ['$Unbound', 0] }, { country: '$_id.country', count: '$Unbound' }, null ] } }, 'Unbound*ByCountry': { '$push': { $cond: [ { $gt: ['$Unbound*', 0] }, { country: '$_id.country', count: '$Unbound*' }, null ] } }, Other: { $sum: '$Other' }, None: { $sum: '$None' }, Unbound: { $sum: '$Unbound' }, 'Unbound*': { $sum: '$Unbound*' } } },
    { $project: { _id: 0, services: '$_id', Other: 1, None: 1, Unbound: 1, 'Unbound*': 1, OtherByCountry: { $setDifference: ['$OtherByCountry', [null]] }, NoneByCountry: { $setDifference: ['$NoneByCountry', [null]] }, UnboundByCountry: { $setDifference: ['$UnboundByCountry', [null]] }, 'Unbound*ByCountry': { $setDifference: ['$Unbound*ByCountry', [null]] } } }
])


// Number of commitments by countries and status
db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
    { $sort: { versionNumber: -1 } },
    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' } } },
    { $unwind: '$sectors' },
    { $project: { countryNameCreator: 1, service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
    { $match: { $or: [ { service: { $in: ['1.A Professional Services', '2.A Postal services'] } }, { cpc_code: { $in: ['833 Engineering services', '6801 Postal services'] } }, { provisional_code: { $in: ['861 Legal services', '9402 Refuse disposal services'] } } ] } },
    { $match: { _id: { $in: ['Madagascar', 'Nigeria', 'Sudan', 'Togo'] } } },
    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
    { $group: { _id: { _id: '$_id', service: '$service', cpc_code: '$cpc_code', provisional_code: '$provisional_code' } } },
    { $project: { _id: '$_id._id', service: '$_id.service', cpc_code: '$_id.cpc_code', provisional_code: '$_id.provisional_code' } },
    { $group: { _id: { _id: '$_id' }, nb_commitments: { '$sum': 1 } } },
    { $project: { country: '$_id._id', nb_commitments: 1, _id: 0 } }
//    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
//    { $sort: { versionNumber: -1 } },
//    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' }, nb_commitments: { '$sum': 1 } } },
//    { $match: { _id: { $in: ['Madagascar', 'Nigeria', 'Sudan', 'Togo', 'Algeria'] } } },
//    { $project: { country: '$_id', _id: 0, nb_commitments: 1 } }
//    { $unwind: '$sectors' },
//    { $match: { _id: { $in: ['Madagascar', 'Nigeria', 'Sudan', 'Togo'] } } },
//    { $project: { commitement: '$sectors.sectorCommitement', service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
])



db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
    { $sort: { versionNumber: -1 } },
//    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' } } },
    { $group: { _id: '$countryNameCreator', nb_commitments: { '$sum': 1 } } },
    { $match: { _id: { $in: ['Togo'] } } },
    { $project: { country: '$_id', _id: 0, nb_commitments: 1 } }
])

db['offer-versions'].aggregate([
    { $match: { status: 'COMMITMENT', versionType: { $in: ['INITIAL', 'UPDATED', 'FINAL'] } } },
    { $sort: { versionNumber: -1 } },
    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' } } },
    { $unwind: '$sectors' },
    { $project: { countryNameCreator: 1, service: { '$arrayElemAt': ['$sectors.services', 0] }, cpc_code: '$sectors.cpc_codes', provisional_code: '$sectors.provisional_codes' } },
    { $unwind: { path: '$cpc_code', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$provisional_code', preserveNullAndEmptyArrays: true } },
    { $match: { _id: { $in: ['Togo', 'Senegal'] } } },
//    { $group: { _id: '$_id', nb_commitments: { '$sum': 1 } } },
//    { $project: { country: '$_id', _id: 0, nb_commitments: 1 } }
])

// two variables 'countries' and 'types'
db['response-offers'].aggregate([
    { $match: { status: 'COMMITMENT' } },
    { $sort: { versionNumber: -1 } },
    { $project: { countryNameCreator: 1, responseCountry: 1 } },
    { $match: { $or: [{ countryNameCreator: { $in: ['Morocco'] } }, { responseCountry: { $in: ['MA'] } }] } }
//    { $group: { _id: '$countryNameCreator', sectors: { $first: '$sectors' } } },
])

//test
db['response-offers'].aggregate([
    { $match: { status: 'COMMITMENT' } },
    { $sort: { versionNumber: -1 } },
    {
        $project: {
            _id: 0,
            countryNameCreator: 1,
            countryIdCreator: 1,
            responseCountry: 1,
            filter: {
                $cond: {
                    if: { $and: [{ $in: ['sent', ['sent']] }, { $in: ['received', ['sent']] }] },
                    then: { $or: [{ $in: ['$countryNameCreator', ['Morocco', 'Algeria']] }, { $in: ['$responseCountry', ['MA', 'DZ']] }] },
                    else: { $cond: { if: { $in: ['sent', ['sent']] }, then: { $in: ['$countryNameCreator', ['Morocco', 'Algeria']] }, else: { $in: ['$responseCountry', ['MA', 'DZ']] } } }
                }
            }
        }
    },
    { $match: { filter: true } },
    { $project: { filter: 0 } },
    {
        $facet: {
            countryIdCreatorCounts: [
                {
                    $match: {
                        countryIdCreator: { $in: ['MA', 'DZ'] }
                    }
                },
                {
                    $group: {
                        _id: "$countryIdCreator",
                        count: { $sum: 1 }
                    }
                },
                {
                    $addFields:{
                        check: { $in: ['sent', ['sent']] }
                    }
                }
            ],
            responseCountryCounts: [
                {
                    $match: {
                        responseCountry: { $in: ['MA', 'DZ'] }
                    }
                },
                {
                    $group: {
                        _id: "$responseCountry",
                        count: { $sum: 1 }
                    }
                },
                {
                    $addFields:{
                        check: { $in: ['received', ['sent']] }
                    }
                }
            ]
        }
    },
    {
        $project: {
            _id: 0,
            counts: {
                $concatArrays: [
                    "$countryIdCreatorCounts",
                    "$responseCountryCounts"
                ]
            }
        }
    },
    {
        $unwind: "$counts"
    },
    {
        $match: {
        "counts.check": true
        }
    },
    {
        $group: {
        _id: "$counts._id",
        nb_offers: { $sum: "$counts.count" }
        }
    },
    { $project: { country: '$_id', _id: 0, nb_offers: 1 } }
//    { $group: { _id: { countryIdCreator: '$countryIdCreator', responseCountry: '$responseCountry' }, nb_com: { '$sum': 1 } } },
//    { $group: { _id: '$countryNameCreator', nb_commitments: { '$sum': 1 } } },
])


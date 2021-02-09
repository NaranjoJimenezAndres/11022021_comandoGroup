

/*Las ventas de cada mes y año, la cantidad maxima del pedido y de articulos*/


db.ventas.aggregate(
    [
      {
        $group:{
            _id: { month: { $month: "$Fecha"},
             year: { $year: "$Fecha" }},
            Ventas:{ $addToSet: "$_id" },
            maxPedido: { $max: { $multiply: [ 
                                    "$Precio €", 
                                    "$Nº UD" ] 
                                } },
            maxCantidad: { $max: "$Nº UD" }
        }},
        
    {
         $project : {
            mes:"$_id.month", 
            año: "$_id.year", 
            _id: 0, Ventas :1, 
            maxPedido :1, 
            maxCantidad: 1} 
    },{
        $sort:{año:1, mes :1}
    }])

/*{ "Ventas" : [ 7, 4 ], "maxPedido" : 3829, "maxCantidad" : 7, "mes" : 5, "año" : 2019 }
{ "Ventas" : [ 2 ], "maxPedido" : 3260, "maxCantidad" : 5, "mes" : 8, "año" : 2019 }
{ "Ventas" : [ 3, 1 ], "maxPedido" : 1010, "maxCantidad" : 2, "mes" : 8, "año" : 2020 }*/



/*La fecha completa de los dias que se vendieron en 2020*/

db.ventas.aggregate([{$match: { $expr: { 
        $eq: [{ $year: "$Fecha" }, 2020] } 
    } },
    {$group: {
        _id: {
            month: { $month: "$Fecha" },
            day: { $dayOfMonth: "$Fecha" },
            year: { $year: "$Fecha" }
        },
        totalSales: {$sum:{$multiply:["$Precio €", "$Nº UD"]}}}},
    ])

/*{ "_id" : { "month" : 8, "day" : 23, "year" : 2020 }, "totalSales" : 1010 }
{ "_id" : { "month" : 8, "day" : 4, "year" : 2020 }, "totalSales" : 702 }*/


/*Envios realizado con cada empresa de mensajeria y si tiene penalizacion si han transportado menos de 10 articulos*/

db.ventas.aggregate(
        [{
            $match:{ $expr: { $eq: [{ $year: "$Fecha" }, 2019] } }},
            
            {
            $group: {
                _id: "$Transporte",
                total: {$sum: "$Nº UD"},
            }},
        {$project:
            {
            item:1,
            total:1,
            Penalizacion:{
                $cond: {if : {$lte: ["$total",10]}, 
                            then: true, else: false}
            }
        }}
    ])


/*{ "_id" : "NACEX", "total" : 12, "Penalizacion" : false }
{ "_id" : "SEUR", "total" : 9, "Penalizacion" : true }*/
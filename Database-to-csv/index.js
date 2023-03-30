const sql = require('mssql');
const fs = require("node:fs/promises")
const csv = require('csv-stringify')

let config = {
    server: 'localhost',
    authentication: {
        type: 'default',
        options: {
            userName: 'login2', // update me
            password: '123' // update me
        }
    },
    options: {
        database: 'test',
        validateBulkLoadParameters:false,
        encrypt: false,
    }
};
  
( async ()=>

{
    // connect to your database
    sql.connect(config, async (err) => {
        
        if (err) console.log(err);
        else {

            console.time('weatherPipe')

            const request = new sql.Request()
            request.stream = true

            request.query("SELECT * FROM MOCK_DATA")

            const writeHandler = await fs.open('weather_data.csv','w')

            const writeStream = writeHandler.createWriteStream()

            writeStream.write("id,timestamp,humidity,wind_speed,precipitation"+"\n")

            request.on('row',row=>{

                if(!writeStream.write(row.id+","+row.timestamp+","+row.humidity+","+row.wind_speed+","+row.precipitation+"\n"))
                {
                    request.pause()
                }

            })

            writeStream.on('drain',()=>{
                request.resume()
            })

            request.on('done',()=>{

                console.timeEnd('weatherPipe')
            })

        }
        
    });  
}

)()

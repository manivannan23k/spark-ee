
const DataService = {
    getAoiByCode: (aoi) => {
        return new Promise((resolve, reject) => {
            fetch(`http://localhost:8082/getAoiByCode?aoiCode=${aoi}`)
                .then(r => r.json())
                .then(r => {
                    return resolve(r)
                })
                .catch(er => {
                    console.log(er)
                    return reject(er);
                })
        })
    },
    addAoi: (aoiGj, aoiName) => {
        return new Promise((resolve, reject) => {
            fetch(`http://localhost:8082/addAoi`, {
                method: "POST",
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    aoiGj: aoiGj,
                    aoiName: aoiName
                })
            })
                .then(r => r.json())
                .then(r => {
                    return resolve(r)
                })
                .catch(er => {
                    console.log(er)
                    return reject(er);
                })
        })
    }
}
export default DataService;
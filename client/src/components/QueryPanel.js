import * as React from 'react';
import Box from '@mui/material/Box';
import TextField from '@mui/material/TextField';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select, { SelectChangeEvent } from '@mui/material/Select';
import DatePicker from 'react-datepicker';
import { Button, Slider, Typography } from '@mui/material';
import { useDispatch } from 'react-redux';
import { addLayer, setQueryResults, setTimeIndexes, toggleAddLayerDialog, toggleQueryResultsDialog, updateRGBBands } from '../actions';


const QueryPanel = (props) => {

    const dispatch = useDispatch()

    const [datasets, setDatasets] = React.useState([])


    const [sensor, setSensor] = React.useState("LISS3")
    // const [fromDate, setFromDate] = React.useState(new Date("1991-01-01"))
    // const [toDate, setToDate] = React.useState(new Date("1991-01-02"))
    const [fromDate, setFromDate] = React.useState(new Date("2017-11-19"))
    const [toDate, setToDate] = React.useState(new Date("2017-11-21"))
    const [aoi, setAoi] = React.useState('aoi_uk_1')

    const [redBand, setRedBand] = React.useState(4)
    const [greenBand, setGreenBand] = React.useState(3)
    const [blueBand, setBlueBand] = React.useState(2)
    const [vizMaxValue, setVizMaxValue] = React.useState(15000)

    const [timeValues, setTimeValues] = React.useState(null)

    React.useEffect(() => {
        // dispatch(updateRGBBands([redBand, greenBand, blueBand]))
    }, [redBand, greenBand, blueBand, vizMaxValue])

    const updateViz = () => {
        dispatch(updateRGBBands({
            bands: [redBand, greenBand, blueBand],
            vizMaxValue: vizMaxValue
        }))
    }

    const getDatasets = () => {
        fetch(`http://localhost:8082/getDatasets`)
            .then(r => r.json())
            .then(r => {
                setDatasets(r.data.filter(e => e.is_querable).map(d => {
                    return {
                        name: d.ds_name,
                        bands: JSON.parse(d.band_meta),
                        style: JSON.parse(d.def_color_scheme),
                        id: d.dataset_id,
                        description: d.ds_description,
                        vmin: d.vmin,
                        vmax: d.vmax,
                        noOfBands: d.no_of_bands
                    }
                }))
            })
            .catch(er => console.log(er))
    }

    React.useEffect(() => {
        getDatasets()
    }, [])

    const queryDataset = () => {
        fetch(`http://localhost:8082/getTimeIndexes?sensorName=${sensor}&fromTs=${fromDate.getTime()}&toTs=${toDate.getTime()}&aoi_code=${aoi}`)
            .then(r => r.json())
            .then(r => {
                console.log(r)
                if (r.data.length > 0) {
                    dispatch(toggleAddLayerDialog(false))
                    dispatch(setQueryResults(r.data))
                    dispatch(toggleQueryResultsDialog(true))
                } else {
                    alert("No result")
                }
            })
            .catch(e => {
                console.log(e)
            })
    }

    return <>
        <Typography variant="h6" gutterBottom component="div">
            Query Datasets
        </Typography>
        <FormControl fullWidth>
            <InputLabel id="sensor-select">Sensor</InputLabel>
            <Select
                labelId="sensor-select"
                value={sensor}
                label="Sensor"
                onChange={(e) => { setSensor(e.target.value) }}
            >
                {
                    datasets.map(ds => {
                        return <MenuItem key={ds.name} value={ds.name}>{`${ds.name} - ${ds.description}`}</MenuItem>
                    })
                }
            </Select>
        </FormControl>
        <br /><br />
        <FormControl fullWidth>
            <InputLabel id="aoi-select">AOI</InputLabel>
            <Select
                labelId="aoi-select"
                value={aoi}
                label="AOI"
                onChange={(e) => { setAoi(e.target.value) }}
            >
                <MenuItem value={'aoi_RJ_Park_small'}>AOI Rajaji Park (Small)</MenuItem>
                <MenuItem value={'aoi_RJP_Field'}>AOI Rajaji Park (Field)</MenuItem>
                <MenuItem value={'aoi_shilma'}>AOI Shimla</MenuItem>
                <MenuItem value={'aoi_haldwani_tw'}>AOI Haldwani Town</MenuItem>
                <MenuItem value={'aoi_haldwani_outer'}>AOI Haldwani Outer</MenuItem>
                <MenuItem value={'aoi_RJ_Park'}>AOI Rajaji Park</MenuItem>
                <MenuItem value={'aoi_uk_1'}>AOI UK</MenuItem>

            </Select>
        </FormControl>
        <br /><br />
        <Typography variant="body2">From date</Typography>
        <DatePicker
            style={{
                "padding": 8
            }}
            selected={fromDate}
            onChange={(date) => setFromDate(date)}
        />
        <br />
        <Typography variant="body2">To date</Typography>
        <DatePicker
            style={{
                "padding": 8
            }}
            selected={toDate}
            onChange={(date) => setToDate(date)}
        />
        <br />

        <br />
        <Button variant="contained" onClick={queryDataset}>Submit</Button>
        <br />

        {/* <Typography variant="h6" gutterBottom component="div">
            Visualization
        </Typography>
        <FormControl fullWidth>
            <InputLabel id="red-band-select">Red band</InputLabel>
            <Select
                labelId="red-band-select"
                value={redBand}
                label="Red band"
                onChange={(e) => { setRedBand(e.target.value) }}
            >
                {
                    Array(7).fill(0).map((e, i) => i).map(e => {
                        return <MenuItem key={e} value={e + 1}>Band {e + 1}</MenuItem>
                    })
                }
            </Select>
        </FormControl>
        <br />
        <br />
        <FormControl fullWidth>
            <InputLabel id="green-band-select">Green band</InputLabel>
            <Select
                labelId="green-band-select"
                value={greenBand}
                label="Green band"
                onChange={(e) => { setGreenBand(e.target.value) }}
            >
                {
                    Array(7).fill(0).map((e, i) => i).map(e => {
                        return <MenuItem key={e} value={e + 1}>Band {e + 1}</MenuItem>
                    })
                }
            </Select>
        </FormControl> */}
        {/* <br />
        <br />
        <FormControl fullWidth>
            <InputLabel id="blue-band-select">Blue band</InputLabel>
            <Select
                labelId="blue-band-select"
                value={blueBand}
                label="Blue band"
                onChange={(e) => { setBlueBand(e.target.value) }}
            >
                {
                    Array(7).fill(0).map((e, i) => i).map(e => {
                        return <MenuItem key={e} value={e + 1}>Band {e + 1}</MenuItem>
                    })
                }
            </Select>
        </FormControl>
        <br />
        <br />
        <FormControl fullWidth>
            <TextField type={"number"} value={vizMaxValue} label="Max. value" variant="outlined" onChange={(e) => setVizMaxValue(e.target.value)} />
        </FormControl>
        <br />
        <br />
        <Button variant="contained" onClick={updateViz}>Update</Button> */}

    </>
}

export default QueryPanel;
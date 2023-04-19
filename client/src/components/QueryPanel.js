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
    const [sensor, setSensor] = React.useState("Landsat_OLI")
    const [fromDate, setFromDate] = React.useState(new Date("2018-12-10"))
    const [toDate, setToDate] = React.useState(new Date("2018-12-30"))
    const [aoi, setAoi] = React.useState('0000000000000000')

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

    const queryDataset = () => {
        fetch(`http://localhost:8082/getTimeIndexes?sensorName=${sensor}&fromTs=${fromDate.getTime()}&toTs=${toDate.getTime()}&aoi_code=${aoi}`)
            .then(r => r.json())
            .then(r => {
                console.log(r)
                if(r.data.length>0){
                    dispatch(toggleAddLayerDialog(false))
                    dispatch(setQueryResults(r.data))
                    dispatch(toggleQueryResultsDialog(true))
                }else{
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
                <MenuItem value={'Landsat_OLI'}>Landsat8</MenuItem>
                <MenuItem value={'SingleRasterBand'}>SingleRasterBand</MenuItem>
            </Select>
        </FormControl>
        <br/><br/>
        <FormControl fullWidth>
            <InputLabel id="aoi-select">AOI</InputLabel>
            <Select
                labelId="aoi-select"
                value={aoi}
                label="AOI"
                onChange={(e) => { setAoi(e.target.value) }}
            >
                <MenuItem value={'uTUYvVGHgcvchgxc'}>AOI 1</MenuItem>
                <MenuItem value={'qwertyuiopasdfgh'}>AOI 2</MenuItem>
                <MenuItem value={'wwertyuiopasdfgh'}>AOI 3</MenuItem>
                <MenuItem value={'0000000000000000'}>AOI 4</MenuItem>
                
            </Select>
        </FormControl>
        <br/><br/>
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
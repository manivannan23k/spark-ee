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
import { addLayer, setTimeIndexes, updateRGBBands } from '../actions';


const QueryPanel = (props) => {

    const dispatch = useDispatch()
    const [sensor, setSensor] = React.useState("Landsat")
    const [fromDate, setFromDate] = React.useState(new Date("2023-01-20"))
    const [toDate, setToDate] = React.useState(new Date("2023-01-28"))

    const [redBand, setRedBand] = React.useState(4)
    const [greenBand, setGreenBand] = React.useState(3)
    const [blueBand, setBlueBand] = React.useState(2)
    const [vizMaxValue, setVizMaxValue] = React.useState(0.75)

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
        fetch(`http://localhost:8082/getTimeIndexes?sensorName=Landsat&fromTs=${fromDate.getTime()}&toTs=${toDate.getTime()}`)
            .then(r => r.json())
            .then(r => {
                console.log(r)
                setTimeValues(r.data.ts)
                dispatch(setTimeIndexes(
                    r.data.tIndexes.map((ti, i) => {
                        return { ts: r.data.ts[i], tIndex: ti }
                    })
                ))
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
                <MenuItem value={'Landsat'}>Landsat8</MenuItem>
            </Select>
            <br />
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
        </FormControl>
        <br />

        <Typography variant="h6" gutterBottom component="div">
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
        </FormControl>
        <br />
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
        <Button variant="contained" onClick={updateViz}>Update</Button>

    </>
}

export default QueryPanel;
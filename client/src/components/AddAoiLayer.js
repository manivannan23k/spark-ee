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
import DataService from '../services.js/Data';


const AddAoiLayer = (props) => {

    const dispatch = useDispatch()

    const [datasets, setDatasets] = React.useState([])
    const [aoi, setAoi] = React.useState("")

    const getDatasets = () => {
        fetch(`http://localhost:8082/getAois`)
            .then(r => r.json())
            .then(r => {
                setDatasets(r.data.map(d => {
                    return {
                        name: d.aoi_name,
                        id: d.id,
                        code: d.aoi_code
                    }
                }))
            })
            .catch(er => console.log(er))
    }

    const getAoiByCode = () => {
        DataService.getAoiByCode(aoi)
            .then(r => {
                dispatch(addLayer({
                    type: 'VECTOR',
                    id: `AOI_${aoi}`,
                    active: true,
                    data: JSON.parse(r.data.geom),
                    aoiCode: aoi,
                    name: aoi,
                    sortOrder: 0,
                    showLegend: false,
                    showInLayerList: true
                }))
            }).catch(e => e);
    }

    React.useEffect(() => {
        getDatasets()
    }, [])


    return <>
        <Typography variant="h6" gutterBottom component="div">
            Add AOI
        </Typography>
        <FormControl fullWidth>
            <InputLabel id="aoi-select">Existing AOI</InputLabel>
            <Select
                labelId="aoi-select"
                value={aoi}
                label="Existing AOI"
                onChange={(e) => { setAoi(e.target.value) }}
            >
                {
                    datasets.map(ds => {
                        return <MenuItem key={ds.code} value={ds.code}>{`${ds.name} (${ds.code})`}</MenuItem>
                    })
                }
            </Select>
        </FormControl>
        <br />
        <br />
        <Button variant="contained" onClick={() => { getAoiByCode(); }}>Add</Button>
        <br />
    </>
}

export default AddAoiLayer;
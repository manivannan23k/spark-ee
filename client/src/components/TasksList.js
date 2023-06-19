import * as React from 'react';
import Box from '@mui/material/Box';
import TextField from '@mui/material/TextField';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select, { SelectChangeEvent } from '@mui/material/Select';
import DatePicker from 'react-datepicker';
import { Button, Slider, Tab, Tabs, Typography } from '@mui/material';
import { useDispatch } from 'react-redux';
import { addLayer, setQueryResults, setTimeIndexes, toggleAddAoiDialog, toggleAddLayerDialog, toggleQueryResultsDialog, toggleTasksDialog, updateRGBBands } from '../actions';
import DataService from '../services.js/Data';
import Config from '../config.js';

const TasksList = (props) => {

    const dispatch = useDispatch()

    const [tasks, setTasks] = React.useState([])

    const getTasks = () => {
        fetch(`${Config.DATA_HOST}/process/getAll`)
            .then(r => r.json())
            .then(r => {
                console.log(r)
                setTasks(r.data.map(d => {
                    return {
                        name: d.pname,
                        id: d.pid,
                        status: d.status
                    }
                }))
            })
            .catch(er => console.log(er))
    }

    React.useEffect(() => {
        getTasks()
    }, [])

    return <>
        <Typography variant="h6" gutterBottom component="div">
            Submitted Processes
        </Typography>
        <div style={{ textAlign: 'right' }}>
            <Button onClick={() => {
                getTasks()
            }}>Reload</Button>
        </div>
        <br />
        <Box sx={{ width: '100%', height: 500, overflowY: 'auto', border: '1px solid #333' }}>
            {
                tasks.map((task, tindx) => {
                    return <div style={{ padding: 12, margin: 6, border: '1px solid #888', height: 60 }}>
                        <div style={{ display: 'inline' }}>{tindx + 1}.{task.name} ({task.id})</div>
                        {task.status === 'COMPLETED' ? <div style={{ display: 'inline', float: 'right' }}>
                            <Button onClick={() => {
                                var url = `${Config.DATA_HOST}/process/download?pid=${task.id}`;
                                window.open(url, '_blank');
                            }} >Download</Button>
                        </div> : <div style={{ display: 'inline', float: 'right' }}>{task.status}</div>}
                    </div>
                })
            }
        </Box>
        <br />
        <br />
        {/* <Button variant="contained" onClick={() => {
            dispatch(toggleTasksDialog(false))
        }}>Close</Button> */}
    </>
}

export default TasksList;
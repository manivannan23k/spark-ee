import * as React from 'react';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Typography from '@mui/material/Typography';
import Modal from '@mui/material/Modal';

const style = {
    position: 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
    width: 1200,
    bgcolor: 'background.paper',
    border: '2px solid #000',
    boxShadow: 24,
    p: 4,
    height: 'calc(100vh - 200px)'
};

const AppModal = (props) => {
    const handleOpen = () => props.setFlag(true);
    const handleClose = () => props.setFlag(false);

    if (!props.content) {
        return <></>
    }

    return (
        <div>
            <Button onClick={handleOpen}>{props.btnText}</Button>
            <Modal
                open={props.flag}
                onClose={handleClose}
                aria-labelledby="modal-modal-title"
                aria-describedby="modal-modal-description"
            >
                <Box sx={style}>
                    {props.content}
                </Box>
            </Modal>
        </div>
    );
}

export default AppModal;
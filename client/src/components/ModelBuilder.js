import React, { useRef, useState, useEffect, useCallback } from "react";
import DraggableComponent from "./DraggableComponent";


const ModelBuilder = (props) => {
    /***
     * Block Types:
     * 1) Raster Input - Single band, Multi band, Multi Temporal
     * 2) Raster Operations -
     *  Local: Raster Calc, Focal: Conv. Filters
     *  Temporal: Avg, SavGol
     * 3) Output - 
     */

    const [components, setComponents] = useState([])

    const addComponent = (cType) => {
        setComponents([
            ...components,
            {
                type: 'input'
            }
        ])
    }

    return <div style={{ position: 'absolute', top: 100, right: 100, zIndex: 500, height: 480, width: 600, background: 'white' }}>
        <ul style={{
            listStyle: 'none',
            margin: 0,
            padding: 0
        }}>
            <li style={{
                cursor: 'pointer'
            }} onClick={() => { addComponent('input') }}>Input</li>
        </ul>
        {
            components.map((c, _i) => {
                return <DraggableComponent key={_i} />
            })
        }
    </div>
}

export default ModelBuilder;
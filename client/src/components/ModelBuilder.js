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

    const addComponent = (cType, c) => {
        setComponents([
            ...components,
            {
                type: cType,
                name: c.name
            }
        ])
    }

    return <div style={{ position: 'absolute', top: 100, right: 100, zIndex: 500, height: 480, width: 600, background: 'white' }}>
        <ul style={{
            listStyle: 'none',
            margin: 0,
            padding: 0
        }}>
            {
                [
                    {
                        type: 'input',
                        components: [
                            {
                                name: "Raster Layer"
                            }
                        ]
                    },
                    {
                        type: 'operation',
                        components: [
                            {
                                name: "Pixel Average"
                            }
                        ]
                    },
                    {
                        type: 'output',
                        components: [
                            {
                                name: "Output Layer"
                            }
                        ]
                    },

                ].map(e => {
                    return <li>
                        {e.type}
                        <ul>
                            {
                                e.components.map(c => {
                                    return <li style={{
                                        cursor: 'pointer'
                                    }} onClick={() => { addComponent(e.type, c) }}>{c.name}</li>
                                })
                            }
                        </ul>
                    </li>
                })
            }
        </ul>
        {
            components.map((c, _i) => {
                return <DraggableComponent key={_i} name={c.name} type={c.type} />
            })
        }
    </div>
}

export default ModelBuilder;
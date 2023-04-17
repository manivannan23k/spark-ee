
import * as go from 'gojs';
import { ReactDiagram } from 'gojs-react';
import { useEffect, useRef, useState } from 'react';
import TextEditorSelectBox from './TextEditorSelectBox';
import { connect } from 'react-redux';
import TextEditorSelectBoxLayer from './TextEditorSelectBoxLayer';


const mapStateToProps = (state) => {
    return {
        map: state.MapReducer,
        dialog: state.DialogReducer
    }
}

/**
 * Diagram initialization method, which is passed to the ReactDiagram component.
 * This method is responsible for making the diagram and initializing the model and any templates.
 * The model's data should not be set here, as the ReactDiagram component handles that via the other props.
 */
function initDiagram() {
    const $ = go.GraphObject.make;
    const templateMap = new go.Map();
    const nodeTemplateInputBand = 
    $(
        go.Node, "Position", {width: 200, height: 100},
        new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
        $(go.Shape, 'Rectangle', {width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
        $(go.TextBlock, {position: new go.Point(0, 0), margin: 0, font: 'bold 14pt serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay()),
        $(go.TextBlock, {position: new go.Point(0, 30), margin: 10, text: "Select Layer: ", stroke: "red", editable: false, width: 200 }),
        $(go.TextBlock, {position: new go.Point(80, 30), margin: 10, text: "-", stroke: "red", editable: true, width: 200 }, new go.Binding('choices','lchoices'), new go.Binding('textEdited', 'layerEdited')),
        $(go.TextBlock, {position: new go.Point(0, 60), margin: 10, text: "Select Band: ", stroke: "red", editable: false, width: 200 }),
        $(go.TextBlock, {position: new go.Point(80, 60), margin: 10 ,text: "-", stroke: "red", editable: true, width: 200 }, new go.Binding('choices','bchoices'), new go.Binding('textEdited', 'bandEdited')),
        makePort("InBand", 200, 10, true, false, [0, 90], 1, 0, $),
        // makePort("L", go.Spot.Left, go.Spot.LeftSide, true, true, $),
        // makePort("R", go.Spot.Right, go.Spot.RightSide, true, true, $),
        // makePort("B", go.Spot.Bottom, go.Spot.BottomSide, true, false, $)
    )
    const nodeTemplateOpNDI = 
    $(
        go.Node, "Position", {width: 200, height: 100},
        new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
        $(go.Shape, 'Rectangle', {width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
        $(go.TextBlock, {position: new go.Point(0, 0), margin: 10, font: 'bold 14pt serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay()),
        $(go.TextBlock, {position: new go.Point(0, 50), margin: 10, text: "NDVI", stroke: "red", editable: false, width: 200 }),
        makePort("OpNDI", 200, 10, false, true, [0, 0], 0, 1, $),
        makePort("OpNDI", 200, 10, true, false, [0, 90], 1, 0, $),
    );
    const nodeTemplateOutBand = 
    $(
        go.Node, "Position", {width: 200, height: 100},
        new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
        $(go.Shape, 'Rectangle', {width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
        $(go.TextBlock, {position: new go.Point(0, 30), margin: 10, font: 'bold 14pt serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay()),
        makePort("OutBand", 200, 10, false, true, [0, 0], 0, 1, $),
    );

    templateMap.add('', nodeTemplateInputBand);
    templateMap.add('opNDI', nodeTemplateOpNDI);
    templateMap.add('outRasterband', nodeTemplateOutBand)

    const diagram =
      $(go.Diagram,
        {
            "textEditingTool.defaultTextEditor": TextEditorSelectBox,
            'undoManager.isEnabled': true,  // must be set to allow for model change listening
          // 'undoManager.maxHistoryLength': 0,  // uncomment disable undo/redo functionality
            'clickCreatingTool.archetypeNodeData': { text: 'new node', color: 'lightblue' },
            model: new go.GraphLinksModel(
            {
              linkKeyProperty: 'key'
            })
        });
  
    diagram.nodeTemplateMap = templateMap
    
    diagram.addDiagramListener("TextEdited", e => {
        // changeEvt(e)
    });
  
    return diagram;
  }
  

  function makePort(name, width, height, output, input, position, fromMaxLinks, toMaxLinks, $) {
    // var horizontal = align.equals(go.Spot.Top) || align.equals(go.Spot.Bottom);
    // the port is basically just a transparent rectangle that stretches along the side of the node,
    // and becomes colored when the mouse passes over it
    return $(go.Shape,
      {
        fill: "rgba(70,70,70,0.5)",  // changed to a color in the mouseEnter event handler
        strokeWidth: 0,  // no stroke
        width: width,//horizontal ? NaN : 8,  // if not stretching horizontally, just 8 wide
        height: height,//!horizontal ? NaN : 8,  // if not stretching vertically, just 8 tall
        // alignment: align,  // align the port on the main Shape
        // stretch: (horizontal ? go.GraphObject.Horizontal : go.GraphObject.Vertical),
        portId: name,  // declare this object to be a "port"
        // fromSpot: spot,  // declare where links may connect at this port
        fromLinkable: output,  // declare whether the user may draw links from here
        // toSpot: spot,  // declare where links may connect at this port
        toLinkable: input,  // declare whether the user may draw links to here
        cursor: "pointer",  // show a different cursor to indicate potential link point
        mouseEnter: (e, port) => {  // the PORT argument will be this Shape
          if (!e.diagram.isReadOnly) port.fill = "rgba(255,0,255,0.5)";
        },
        mouseLeave: (e, port) => port.fill = "rgba(70,70,70,0.5)", //"transparent",
        position: new go.Point(...position),
        toMaxLinks: toMaxLinks,fromMaxLinks: fromMaxLinks,
      });
  }

const GoDiagram = (props) => {

    const layers = Object.keys(props.map.layers).filter(lk=>{
        return props.map.layers[lk].type==='DATA_TILE'
    }).map(k=>props.map.layers[k]);

    function handleModelChange(changes) {
        props.modelChange(changes)
    }

    const diagramRef = useRef()
    const [nodeArray, setNodeArray] = useState([
        // { key: 0, text: 'In Raster Band', color1: 'white', loc: '0 0', defaultBand: 'Band 1',  choices: ['Band 1', 'Band 2', 'Band 3', 'Band 4'] },
        // { key: 1, text: 'In Raster Band', color1: 'white', loc: '250 0', defaultBand: 'Band 1',  choices: ['Band 1', 'Band 2', 'Band 3', 'Band 4'] },
        // { key: 2, text: 'Algorithm', color1: 'white', loc: '100 150', defaultBand: 'NDVI', category: 'opNDI' },
        // { key: 3, text: 'Out Raster Band', color1: 'white', loc: '100 300', defaultBand: 'Band 1',  choices: ['Band 1', 'Band 2', 'Band 3', 'Band 4'] },
        // { key: 1, text: 'Beta', color: 'orange', loc: '150 0' },
        // { key: 2, text: 'Gamma', color: 'lightgreen', loc: '0 150' },
        // { key: 3, text: 'Delta', color: 'pink', loc: '150 150' }
    ])

    useEffect(()=>{
        if(props.components){
            let nodes = [];
            for (let i = 0; i < props.components.length; i++) {
                let node = null;
                const component = props.components[i];
                switch(component.type){
                    case "in_raster_band":
                        node = { 
                            key: i,
                            text: component.name,
                            color1: 'white',
                            loc: '0 0',
                            defaultBand: 'Band 1',
                            bchoices: Array(7).fill(0).map((e, i)=>`Band ${i+1}`),
                            lchoices: layers.map(l=>l.name),
                            bandEdited: (e)=>{handleModelChange({
                                id: component.id, value: e.text, type: 'inBandEdit'
                            })},
                            layerEdited: (e)=>{handleModelChange({
                                id: component.id, value: e.text, type: 'inLayerEdit'
                            })}
                        }
                        break;
                    case "out_raster_band":
                        node = { key: i, text: component.name, color1: 'white', loc: '0 0', category: 'outRasterband'}
                        break;
                    default:
                        node = { key: i, text: 'Algorithm', color1: 'white', loc: '100 150', defaultBand: 'NDVI', category: 'opNDI' }
                        break;
                }
                nodes.push(node)
            }
            // console.log(nodes)
            setNodeArray(nodes);
        }
    }, [props.components])


    return <div style={{width: '100%', height: '100%', backgroundColor: 'grey'}}>
        <ReactDiagram style={{width: '100%', height: '100%', backgroundColor: 'grey'}}
            ref={diagramRef}
            initDiagram={initDiagram}
            divClassName='diagram-component'
            nodeDataArray={nodeArray}
            linkDataArray={[
            // { key: -1, from: 0, to: 2 },
            // { key: -2, from: 1, to: 2 },
            // { key: -3, from: 2, to: 3 },
            // { key: -2, from: 0, to: 2 },
            // { key: -3, from: 1, to: 1 },
            // { key: -4, from: 2, to: 3 },
            // { key: -5, from: 3, to: 0 }
            ]}
            onModelChange={handleModelChange}

        />
    </div>
}


export default connect(mapStateToProps)(GoDiagram);
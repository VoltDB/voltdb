import React, { Component } from 'react';
import PropTypes from 'prop-types';
import Odometer from 'odometer';
import ReactDOM from 'react-dom';

import '../components-css/odometer.css';

class OdometerInstance extends Component {
    static propTypes = {
        value: PropTypes.number.isRequired,
        options: PropTypes.object,
    };

    componentDidMount() {
        this.odometer = new Odometer({
            el: ReactDOM.findDOMNode(this),
            value: this.props.value, ...this.props.options,
        });
    }

    componentDidUpdate() {
        this.odometer.update(this.props.value)
    }

    render() {
        return React.createElement('div');
    }
}

export default OdometerInstance;
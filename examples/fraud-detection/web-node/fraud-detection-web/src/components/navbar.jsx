import React, { Component } from 'react';

import Navbar from 'react-bootstrap/lib/Navbar';
import NavItem from 'react-bootstrap/lib/NavItem';
import Nav from 'react-bootstrap/lib/Nav';
import MenuItem from 'react-bootstrap/lib/MenuItem';
import NavDropdown from 'react-bootstrap/lib/NavDropdown';

import '../components-css/navbar.css';
import logo from '../resources/logo.png';

// The navigation bar, based on React-Bootstrap
class NavbarInstance extends Component {
  render() {
    return (
      <Navbar collapseOnSelect>
        <Navbar.Header>
          <Navbar.Brand>
            <a href="#"><img alt='Boom' src={logo} height="40" /></a>
          </Navbar.Brand>
          <Navbar.Toggle />
        </Navbar.Header>
        <Navbar.Collapse>
          <Nav>
            <NavItem eventKey={1} href="#">Fraud Monitoring System</NavItem>
            <NavDropdown eventKey={3} title="Show" id="basic-nav-dropdown">
              <MenuItem eventKey={3.1}>Tab1</MenuItem>
              <MenuItem eventKey={3.2}>Tab2</MenuItem>
              <MenuItem eventKey={3.3}>Tab3</MenuItem>
              <MenuItem divider />
              <MenuItem eventKey={3.3}>Tab4</MenuItem>
            </NavDropdown>
          </Nav>

          <Nav pullRight>
            <NavItem eventKey={1} href="#">VoltDB Management Center</NavItem>
          </Nav>
        </Navbar.Collapse>
      </Navbar>
    );
  }
}

export default NavbarInstance;

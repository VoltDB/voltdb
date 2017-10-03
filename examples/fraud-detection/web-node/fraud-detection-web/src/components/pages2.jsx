import React, { Component } from 'react';

import { SectionsContainer, Section, Header } from 'react-fullpage';

// import NavbarInstance from './navbar.jsx';
import BusiestStationsChart from './busychart.jsx';

import logo from '../resources/logo.png';

import '../components-css/pages.css';

class Pages2 extends Component {
    
    render() {
    let options = {
      sectionClassName: 'section',
      scrollBar: false,
      navigation: true,
      verticalAlign: false,
      sectionPaddingTop: '50px',
      sectionPaddingBottom: '50px',
      arrowNavigation: true
    };

    return (
      <div>
        <Header>
          <a href="http://localhost:8080" className='nav-img'><img alt='Boom' src={logo} height="40" /></a>
          <a href="" className='nav-elem elem-right'>Fraud Monitoring System</a>
        </Header>
        <SectionsContainer className="container" {...options}>

          {/* The busy stations chart */}
          <Section color="#A7DBD8">
            <div className='chart-1-title'>
              <p>Busiest Stations</p>
            </div>
            <div className='wrapper1'>
              <BusiestStationsChart busiestStations={this.props.busiestStations} />
            </div>
          </Section>

        </SectionsContainer>
      </div>
    );
  }
}

export default Pages2;

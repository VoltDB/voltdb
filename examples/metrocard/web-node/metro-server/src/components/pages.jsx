import React, { Component } from 'react';

import { SectionsContainer, Section, Header } from 'react-fullpage';

// import NavbarInstance from './navbar.jsx';
import OdometerInstance from './odometer.jsx';
import BusiestStationsChart from './busychart.jsx';
import AvgWaitsChart from './avgwaitschart.jsx';

import logo from '../resources/logo.png';

import '../components-css/pages.css';

class Pages extends Component {
  render() {
    let options = {
      sectionClassName: 'section',
      anchors: ['sectionOne', 'sectionTwo', 'sectionThree', 'sectionFour', 'sectionFive'],
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
          <a href="" className='nav-img'><img alt='Boom' src={logo} height="40" /></a>
          <a href="#sectionOne" className='nav-elem'>Swipes</a>
          <a href="#sectionTwo" className='nav-elem'>Station Stats</a>
          <a href="#sectionThree" className='nav-elem'>Waiting Time</a>
          <a href="#sectionFour" className='nav-elem'>Frauds</a>
          <a href="#sectionFive" className='nav-elem'>Section Five</a>
          <a href="" className='nav-elem elem-right'>Metro Monitoring System</a>
        </Header>
        {/* <Footer>
          <a href="">Dcoumentation</a>
          <a href="">Example Source</a>
          <a href="">About</a>
        </Footer> */}
        <SectionsContainer className="container" {...options}>

          {/* The first odometer for showing # of card swipes */}
          <Section className="custom-section" verticalAlign="true" color="#eee0d3">
            <section className='wrapper1'>
              <div id="one">
                <OdometerInstance value={this.props.num} theme={'car'} />
              </div>
              <div id="two">
                Card Swipes
              </div>
            </section>
          </Section>

          {/* The busy stations chart */}
          <Section color="#A7DBD8">
            <div className='chart-1-title'>
              <p>Busiest Stations</p>
            </div>
            <div className='chart-container'>
              <BusiestStationsChart busiestStations={this.props.busiestStations} />
            </div>
          </Section>

          {/* The avg waiting time chart */}
          <Section color="#E0E4CC">
            <div className='chart-2-container'>
              <AvgWaitsChart avgWaits={this.props.avgWaits} />
            </div>
          </Section>

          {/* The frauds page */}
          <Section className='custom-section' color="#111111">
            <div className='wrapper-odometer-2'>
              <div>
                <OdometerInstance value={this.props.num} theme={'digital'} />
              </div>
            </div>
            <div className='frauds-text'>
              FRAUDS DETECTED
            </div>
          </Section>

          <Section color="#E0E4CC">Section 5</Section>

        </SectionsContainer>
      </div>
    );
  }
}

export default Pages;
import React, { useState, useEffect, useCallback } from "react";
import anime from "animejs";
import "./Hero.css";

const Hero: React.FC = () => {
  const [columns, setColumns] = useState(0);
  const [rows, setRows] = useState(0);
  const [toggled, setToggled] = useState(false);

  const toggle = () => {
    setToggled((prev) => !prev);
    document.body.classList.toggle("toggled");
  };

  const handleOnClick = (index: number) => {
    toggle();
    anime({
      targets: ".tile",
      opacity: toggled ? 1 : 0,
      delay: anime.stagger(50, {
        grid: [columns, rows],
        from: index,
      }),
    });
  };

  const createTiles = useCallback(() => {
    const tileCount = columns * rows;
    return Array.from({ length: tileCount }, (_, index) => (
      <div
        key={index}
        className="tile"
        style={{ opacity: toggled ? 0 : 1 }}
        onClick={() => handleOnClick(index)}
      />
    ));
  }, [columns, rows, toggled]);

  const createGrid = useCallback(() => {
    const size = window.innerWidth > 800 ? 40 : 50;
    setColumns(Math.floor(window.innerWidth / size));
    setRows(Math.floor(window.innerHeight / size));
  }, []);

  useEffect(() => {
    createGrid();
    window.addEventListener("resize", createGrid);
    return () => window.removeEventListener("resize", createGrid);
  }, [createGrid]);

  return (
    <div id="tiles" style={{ display: "grid", gridTemplateColumns: `repeat(${columns}, 1fr)`, gridTemplateRows: `repeat(${rows}, 1fr)` }}>
      {createTiles()}
      
    </div>
  );
};

export default Hero;
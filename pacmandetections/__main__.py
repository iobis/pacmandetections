from pacmandetections import DetectionEngine


def main():
    engine = DetectionEngine(geometry="859b41b3fffffff", speedy_data="~/Desktop/temp/speedy_data")
    detections = engine.generate()
    print(detections)


if __name__ == "__main__":
    main()

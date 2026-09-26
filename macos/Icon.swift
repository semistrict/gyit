import AppKit
import Foundation

// Render the one source image for the macOS iconset. The system supplies the
// familiar peach glyph; all required icon sizes are derived from this image.
let output = URL(fileURLWithPath: CommandLine.arguments[1])
let size = 1024
guard let bitmap = NSBitmapImageRep(bitmapDataPlanes: nil, pixelsWide: size,
                                    pixelsHigh: size, bitsPerSample: 8,
                                    samplesPerPixel: 4, hasAlpha: true, isPlanar: false,
                                    colorSpaceName: .deviceRGB, bytesPerRow: 0,
                                    bitsPerPixel: 0),
      let context = NSGraphicsContext(bitmapImageRep: bitmap) else {
    fatalError("could not allocate peach icon")
}
NSGraphicsContext.saveGraphicsState()
NSGraphicsContext.current = context

NSColor.clear.setFill()
NSRect(x: 0, y: 0, width: size, height: size).fill()

let tile = NSBezierPath(roundedRect: NSRect(x: 32, y: 32, width: 960, height: 960),
                        xRadius: 215, yRadius: 215)
let gradient = NSGradient(starting: NSColor(srgbRed: 0.23, green: 0.17, blue: 0.37, alpha: 1),
                          ending: NSColor(srgbRed: 0.12, green: 0.09, blue: 0.23, alpha: 1))!
gradient.draw(in: tile, angle: -45)

let peach = "🍑" as NSString
let font = NSFont(name: "AppleColorEmoji", size: 710)!
let attributes: [NSAttributedString.Key: Any] = [.font: font]
let bounds = peach.size(withAttributes: attributes)
peach.draw(at: NSPoint(x: (CGFloat(size) - bounds.width) / 2,
                       y: (CGFloat(size) - bounds.height) / 2 + 15),
           withAttributes: attributes)

context.flushGraphics()
NSGraphicsContext.restoreGraphicsState()
guard let png = bitmap.representation(using: .png, properties: [:]) else {
    fatalError("could not render peach icon")
}
try png.write(to: output)
